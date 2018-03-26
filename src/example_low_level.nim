## Example 
##
import nimkafka/rdkafka
import strutils
import tables
#import uuids


echo("librdkafka version: " & $rd_kafka_version_str())

let brokers = "localhost:9092"
let topic_name = "my.test.topic.1"
let timeout_ms: cint = 10000


iterator iterate_partitions(partitions: ptr rd_kafka_topic_partition_list_t): ptr rd_kafka_topic_partition_t  =
  var p = partitions.elems  # pointer to first elem
  for i in 0..<partitions.cnt:
    yield p
    p = cast[ptr type(p[])](cast[ByteAddress](p) + sizeof(p[]))  # do some pointer arithmetic to step forward


proc print_partitions(partitions: ptr rd_kafka_topic_partition_list_t) =
  for p in iterate_partitions(partitions):
    echo("(topic, partition, offset) = ($1, $2, $3)" % [$p.topic, $p.partition, $p.offset])


proc print_watermarks_and_positions(rk: ptr rd_kafka_t; partitions: ptr rd_kafka_topic_partition_list_t) =
  # fetch current offsets
  var err = rd_kafka_position(rk, partitions)
  if err != rd_kafka_resp_err_t.RD_KAFKA_RESP_ERR_NO_ERROR:
    echo("Cannot retrieve current positions")
    echo($rd_kafka_err2str(err))
    quit(1)

  for p in iterate_partitions(partitions):
    echo("Watermarks for topic=$1 partition=$2.  Offset $3 means invalid" % [$p.topic, $p.partition, $RD_KAFKA_OFFSET_INVALID])
    # get offsets that are locally cached
    var lo, hi: int64
    var err = rd_kafka_get_watermark_offsets(rk, p.topic, p.partition, addr lo, addr hi)
    if err != rd_kafka_resp_err_t.RD_KAFKA_RESP_ERR_NO_ERROR:
      echo("  Cannot get watermarks")
      echo($rd_kafka_err2str(err))
      quit(1)
    echo("  Last-seen ($1, $2)" % [$lo, $hi]) 

    # reach out to the broker to query offsets
    err = rd_kafka_query_watermark_offsets(rk, p.topic, p.partition, addr lo, addr hi, timeout_ms)
    if err != rd_kafka_resp_err_t.RD_KAFKA_RESP_ERR_NO_ERROR:
      echo("  Cannot query watermarks")
      echo($rd_kafka_err2str(err))
      quit(1)
    echo("  Queried ($1, $2)" % [$lo, $hi]) 

    echo("  Current position=$1" % [$p.offset])


proc logger(rk: ptr rd_kafka_t; level: cint; fac, buf: cstring) {.cdecl.} =
  echo "$1:$2:$3" % [$rd_kafka_name(rk), $fac, $buf]


proc rebalance_cb(rk: ptr rd_kafka_t; 
                  err: rd_kafka_resp_err_t;
                  partitions: ptr rd_kafka_topic_partition_list_t; 
                  opaque: pointer) {.cdecl.} =
  # Custom rebalance callback (this is optional just for illustrative purposes)
  #
  if err == rd_kafka_resp_err_t.RD_KAFKA_RESP_ERR_ASSIGN_PARTITIONS:
    echo("Reassigning to the following partitions:")
    print_partitions(partitions)
    discard rd_kafka_assign(rk, partitions)
  else:
    echo("Revoking the following partitions:")
    print_partitions(partitions)
    discard rd_kafka_assign(rk, nil)


proc configure_kafka(config: TableRef): ptr rd_kafka_conf_t =
  result = rd_kafka_conf_new()
  rd_kafka_conf_set_log_cb(result, logger)  # optionally, set custom logger
  rd_kafka_conf_set_rebalance_cb(result, rebalance_cb)  # optionally, set custom rebalancer

  var errstr = newString(512)
  for key, val in config.pairs: 
    if rd_kafka_conf_set(result, key, val, errstr, 512) != rd_kafka_conf_res_t.RD_KAFKA_CONF_OK:
      echo("Error setting kafka configuration")
      echo(errstr)
      quit(1)


proc configure_topics(config: TableRef): ptr rd_kafka_topic_conf_t =
  result = rd_kafka_topic_conf_new()

  var errstr = newString(512)
  for key, val in config.pairs: 
    if rd_kafka_topic_conf_set(result, key, val, errstr, 512) != rd_kafka_conf_res_t.RD_KAFKA_CONF_OK:
      echo("Error setting default topic configuration")
      echo(errstr)
      quit(1)


proc destroy_kafka(rk: ptr rd_kafka_t) =
  # Waits until kafka backgrounds threads have cleaned themselves up
  #
  rd_kafka_destroy(rk)

  var counter = 5
  while (counter > 0 and rd_kafka_wait_destroyed(timeout_ms) == -1):
    counter -= 1
  if counter <= 0:
    echo("Kafka didn't clean itself up in time!")
    rd_kafka_dump(addr stdout, rk)


proc produce() =
  # Creates a producer, sends some test messages, then destroys the producer
  #
  echo("Configuring producer")
  var 
    config = {"api.version.request": "true",
              "log.connection.close": "false",
              "socket.keepalive.enable": "true",
              "session.timeout.ms": "10000"}.newTable
    topic_config = {"offset.store.method": "broker",
                    "auto.offset.reset": "smallest"}.newTable
  let conf = configure_kafka(config)
  let topic_conf = configure_topics(topic_config)

  var errstr = newString(512)
  echo("Creating producer")
  let kp = rd_kafka_new(rd_kafka_type_t.RD_KAFKA_PRODUCER, conf, errstr, 512)
  if isNil(kp):
    echo "Failed to create producer"
    echo($errstr)
    quit(1)

  # add brokers
  var numbrokers = rd_kafka_brokers_add(kp, brokers)
  if numbrokers == 0:
    echo("No valid brokers specified")
    quit(1)
  else:
    echo("Brokers added: " & $numbrokers)

  # create topic
  let topic = rd_kafka_topic_new(kp, topic_name, topic_conf)
 
  let key: cstring = nil
  let keylen = 0
  for i in 1..10:
    var message:cstring = "test message! " & $i 
    echo("Producing $1" % [$message])
    
    var res = rd_kafka_produce(topic,
                               RD_KAFKA_PARTITION_UA,  # use automatic partitioning
                               RD_KAFKA_MSG_F_COPY,  # copy data
                               message,
                               message.len+1,
                               key,
                               keylen,
                               nil)

    if res != 0:
      echo "produce resulted in an error"

  echo("Flushing producer queue")
  discard rd_kafka_flush(kp, timeout_ms)

  destroy_kafka(kp)

 
proc message_to_str(m: ptr rd_kafka_message_t): cstring =
  # Unpacks a raw message, prints metadata, and returns the payload
  #
  echo("message partition: " & $m.partition)
  echo("        len: " & $m.len)
  echo("        offset: " & $m.offset)

  let err = rd_kafka_message_errstr(m)
  if err != nil or err.len > 0:
    echo("        error: " & $err)
    return nil
    
  var res = cast[cstring](m.payload)
  result = res
  

proc consume() =
  # Creates a consumer, subscribes to a topic, consumes, then destroys the consumer
  #
  echo("Configuring consumer")
  var 
    #groupid = $genUUID()
    groupid = "test.consumer.1"
    config = {"group.id": groupid,
              "api.version.request": "true",
              "log.connection.close": "false",
              "socket.keepalive.enable": "true",
              "session.timeout.ms": "10000"}.newTable
    topic_config = {"offset.store.method": "broker",
                    "auto.offset.reset": "smallest"}.newTable
  echo("Setting consumer group to $1" % groupid)
  let conf = configure_kafka(config)
  let topic_conf = configure_topics(topic_config)

  echo("Setting default topic configuration")
  rd_kafka_conf_set_default_topic_conf(conf, topic_conf)

  var errstr = newString(512)
  echo("Creating consumer")
  let kc = rd_kafka_new(rd_kafka_type_t.RD_KAFKA_CONSUMER, conf, errstr, 512)
  if isNil(kc):
    echo "Failed to create consumer"
    echo($errstr)
    quit(1)

  # add brokers
  var numbrokers = rd_kafka_brokers_add(kc, brokers)
  if numbrokers == 0:
    echo("No valid brokers specified")
    quit(1)
  else:
    echo("Brokers added: " & $numbrokers)

  echo("Creating list of topics/partitions")
  let num_partitions: cint = 1
  let partition: cint = RD_KAFKA_PARTITION_UA  # want partitions to be auto-assigned (i.e. balanced consumer), so going to use subscribe api (not assign api)
  var partitions = rd_kafka_topic_partition_list_new(num_partitions)
  discard rd_kafka_topic_partition_list_add(partitions, topic_name, partition)

  #err = rd_kafka_committed(kc, partitions, 10000)
  #if err != rd_kafka_resp_err_t.RD_KAFKA_RESP_ERR_NO_ERROR:
  #  echo "Could not fetch offsets from broker"
  #  echo $rd_kafka_err2str(err)
  #  quit(1)

  echo("Subscribing")
  var err = rd_kafka_subscribe(kc, partitions)
  if err != rd_kafka_resp_err_t.RD_KAFKA_RESP_ERR_NO_ERROR:
    # NOTE: for some reason never seems to get here
    echo("Could not subscribe to partitions")
    echo($rd_kafka_err2str(err))
    quit(1)

  # grab a single message
  var message = rd_kafka_consumer_poll(kc, timeout_ms)

  # what is our assignment? (MUST previously poll once because assignment is lazy)
  echo("Grabbing partition assignment")
  err = rd_kafka_assignment(kc, addr partitions)
  if err != rd_kafka_resp_err_t.RD_KAFKA_RESP_ERR_NO_ERROR:
    echo("Could not retrieve partition assignment")
    echo($rd_kafka_err2str(err))
    quit(1)

  if partitions.cnt == 0:
    echo("Assigned no partitions, so exiting")
    quit(1)    

  # see where we are
  print_watermarks_and_positions(kc, partitions)

  # loop getting messages until empty
  echo("Consuming messages")
  while not isNil(message):
    var payload = message_to_str(message)
    if not isNil(payload):
      echo("        payload: " & $payload)
    rd_kafka_message_destroy(message)
    message = rd_kafka_consumer_poll(kc, timeout_ms)

  # see where we are now
  print_watermarks_and_positions(kc, partitions)

  # stop consuming
  echo("Stopping consumer")
  err = rd_kafka_consumer_close(kc)
  if err != rd_kafka_resp_err_t.RD_KAFKA_RESP_ERR_NO_ERROR:
    echo("Cannot close consumer")
    echo($rd_kafka_err2str(err))
    quit(1)
  rd_kafka_topic_partition_list_destroy(partitions)

  destroy_kafka(kc)


produce()
consume()
