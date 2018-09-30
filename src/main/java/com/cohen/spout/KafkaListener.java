package com.cohen.spout;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cohen.common.KeyedQueue;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

/**
 * 负载均衡
 * 每个 Consumer Group 可以包含多个消费实例，即可以启动多个 Kafka Consumer，并把参数 group.id 设置成相同的值。属于同一个 Consumer Group 的消费实例会负载消费订阅的 Topic。
 * 举例：Consumer Group A 订阅了 Topic A，并开启三个消费实例 C1、C2、C3，则发送到 Topic A 的每条消息最终只会传给 C1、C2、C3 的某一个。Kafka 默认会均匀地把消息传给各个消息实例，以做到消费负载均衡。
 * Kafka 负载消费的内部原理是，把订阅的 Topic 的分区，平均分配给各个消费实例。因此，消费实例的个数不要大于分区的数量，否则会有实例分配不到任何分区而处于空跑状态。这个负载均衡发生的时间，除了第一次启动上线之外，后续消费实例发生重启、增加、减少等变更时，都会触发一次负载均衡。
 * 消息队列 Kafka 的每个 Topic 的分区数量默认是 16 个，已经足够满足大部分场景的需求，且云上服务会根据容量调整分区数。
 * <p>
 * 多个订阅
 * 一个 Consumer Group 可以订阅多个 Topic。一个 Topic 也可以被多个 Consumer Group 订阅，且各个 Consumer Group 独立消费 Topic 下的所有消息。
 * 举例：Consumer Group A 订阅了 Topic A，Consumer Group B 也订阅了 Topic A，则发送到 Topic A 的每条消息，不仅会传一份给 Consumer Group A 的消费实例，也会传一份给 Consumer Group B 的消费实例，且这两个过程相互独立，相互没有任何影响。
 * <p>
 * 消费位点
 * 每个 Topic 会有多个分区，每个分区会统计当前消息的总条数，这个称为最大位点 MaxOffset。Kafka Consumer 会按顺序依次消费分区内的每条消息，记录已经消费了的消息条数，称为ConsumerOffset。
 * 剩余的未消费的条数（也称为消息堆积量） = MaxOffset - ConsumerOffset
 * <p>
 * 消费位点提交
 * Kafka 消费者有两个相关参数：
 * enable.auto.commit：默认值为 true。
 * auto.commit.interval.ms： 默认值为 1000，也即 1s。
 * 这两个参数组合的结果就是，每次 poll 数据前会先检查上次提交位点的时间，如果距离当前时间已经超过参数auto.commit.interval.ms规定的时长，则客户端会启动位点提交动作。
 * 因此，如果将enable.auto.commit设置为 true，则需要在每次 poll 数据时，确保前一次 poll 出来的数据已经消费完毕，否则可能导致位点跳跃。
 * 如果想自己控制位点提交，请把 enable.auto.commit 设为 false，并调用 commit(offsets)函数自行控制位点提交。
 * <p>
 * 消息重复和消费幂等
 * Kafka 消费的语义是 “at least once”， 也就是至少投递一次，保证消息不丢，但是不会保证消息不重复。在出现网络问题、客户端重启时均有可能出现少量重复消息，此时应用消费端如果对消息重复比较敏感（比如说订单交易类），则应该做到消息幂等。
 * 以数据库类应用为例，常用做法是：
 * 发送消息时，传入 key 作为唯一流水号ID；
 * 消费消息时，判断 key 是否已经消费过，如果已经消费过了，则忽略，如果没消费过，则消费一次；
 * 当然，如果应用本身对少量消息重复不敏感，则不需要做此类幂等检查。
 * <p>
 * 消费失败
 * Kafka 是按分区一条一条消息顺序向前推进消费的，如果消费端拿到某条消息后执行消费逻辑失败，比如应用服务器出现了脏数据，导致某条消息处理失败，等待人工干预，那么有以下两种处理方式：
 * 失败后一直尝试再次执行消费逻辑。这种方式有可能造成消费线程阻塞在当前消息，无法向前推进，造成消息堆积；
 * 由于 Kafka 自身没有处理失败消息的设计，实践中通常会打印失败的消息、或者存储到某个服务（比如创建一个 Topic 专门用来放失败的消息），然后定时 check 失败消息的情况，分析失败原因，根据情况处理。
 * <p>
 * 消费阻塞以及堆积
 * 消费端最常见的问题就是消费堆积，最常造成堆积的原因是：
 * 消费速度跟不上生产速度，此时应该提高消费速度；
 * 消费端产生了阻塞。
 * 消费端拿到消息后，执行消费逻辑，通常会执行一些远程调用，如果这个时候同步等待结果，则有可能造成一直等待，消费进程无法向前推进。
 * 消费端应该竭力避免堵塞消费线程，如果存在等待调用结果的情况，建议设置等待的超时时间，超时后作消费失败处理。
 * <p>
 * 提高消费速度
 * 提高消费速度有以下两个办法：
 * 增加 Consumer 实例个数
 * 增加消费线程
 * 增加 Consumer 实例
 * 可以在进程内直接增加（需要保证每个实例对应一个线程，否则没有太大意义），也可以部署多个消费实例进程；需要注意的是，实例个数超过分区数量后就不再能提高速度，将会有消费实例不工作。
 * 增加消费线程
 * 增加 Consumer 实例本质上也是增加线程的方式来提升速度，因此更加重要的性能提升方式是增加消费线程，最基本的步骤如下：
 * 定义一个线程池；
 * Poll 数据；
 * 把数据提交到线程池进行并发处理；
 * 等并发结果返回成功后，再次 poll 数据执行。
 * <p>
 * 消息过滤
 * Kafka 自身没有消息过滤的语义。实践中可以采取以下两个办法：
 * 如果过滤的种类不多，可以采取多个 Topic 的方式达到过滤的目的；
 * 如果过滤的种类多，则最好在客户端业务层面自行过滤。
 * 实践中请根据业务具体情况进行选择，也可以综合运用上面两种办法。
 * <p>
 * 消息广播
 * Kafka 自身没有消息广播的语义，可以通过创建不同的 Consumer Group 来模拟实现。
 * <p>
 * 订阅关系
 * 同一个 Consumer Group 内，各个消费实例订阅的 Topic 最好保持一致，避免给排查问题带来干扰。
 *
 * @author 林金成
 * @date 2018/9/26 15:18
 */
public class KafkaListener {
    private static final Logger PLOG = LoggerFactory.getLogger(KafkaListener.class);

    private KafkaConsumer<String, String> consumer;
    private KeyedQueue<JSONObject> taskQueue;

    public KafkaListener(KeyedQueue<JSONObject> taskQueue) {
        this.taskQueue = taskQueue;
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "39.108.182.71:9092,39.108.182.71:9093,39.108.182.71:9094");
        // 两次 poll 之间的最大允许间隔
        // 请不要改得太大，服务器会掐掉空闲连接，不要超过 30000
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 25000);
        // 每次 poll 的最大数量
        // 注意该值不要改得太大，如果 poll 太多数据，而不能在下次 poll 之前消费完，则会触发一次负载均衡，产生卡顿
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 30);
        // 消息的反序列化方式
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // 当前消费实例所属的消费组，请在控制台申请之后填写
        // 属于同一个组的消费实例，会负载消费消息
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Arrays.asList("test"));
    }

    public void start() {
        while (true) {
            try {
                this.emitMessage(this.consumer.poll(2000));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 订单消息写入本地，文件路径提交
     */
    private void emitMessage(ConsumerRecords<String, String> records) {
        for (ConsumerRecord<String, String> record : records) {
            String key = record.key();
            String message = record.value();
            PLOG.info("[Received Message] : offset = ".concat(String.valueOf(record.offset())).concat(", key = ").concat(key).concat(", value = ").concat(message));
            JSONObject messageJson = JSONObject.parseObject(message);
            String pk_groupid = messageJson.containsKey("vgroupcode") ? messageJson.getString("vgroupcode") : "";
            String vscode = messageJson.containsKey("vscode") ? messageJson.getString("vscode") : "";
            String dworkdate = messageJson.containsKey("dworkdate") ? messageJson.getString("dworkdate") : "";
            String vbcode = messageJson.containsKey("vbcode") ? messageJson.getString("vbcode") : "";
            String vorclass = messageJson.containsKey("vorclass") ? messageJson.getString("vorclass") : "";
            System.out.println("receive order:{}" + vbcode);
            if (pk_groupid == null || pk_groupid.isEmpty()) {
                PLOG.info("商户号为空！");
                continue;
            }
            if (vscode == null || vscode.isEmpty()) {
                PLOG.info("门店编码为空！tenantId = {}", pk_groupid);
                continue;
            }
            if (dworkdate == null || dworkdate.isEmpty()) {
                PLOG.info("日期为空！tenantId = {}, vscode = {}", pk_groupid, vscode);
                continue;
            }
            if (vbcode == null || vbcode.isEmpty()) {
                PLOG.info("账单编码为空！tenantId = ".concat(pk_groupid).concat(", vscode = ").concat(vscode).concat(", dworkdate = ").concat(dworkdate));
                continue;
            }
            if (vorclass == null || vorclass.isEmpty()) {
                PLOG.info("账单类型为空！tenantId = ".concat(pk_groupid).concat(", vscode = ").concat(vscode).concat(", dworkdate = ").concat(dworkdate).concat(", vbcode = ").concat(vbcode));
                continue;
            }
            String taskKey = pk_groupid.concat(":").concat(vscode).concat(":").concat(dworkdate);
            this.taskQueue.add(taskKey, JSON.parseObject(message));
            PLOG.info("[Message Emit Success] : tenantId = ".concat(pk_groupid).concat(", vscode = ").concat(vscode).concat(", dworkdate = ").concat(dworkdate).concat(", vbcode = ").concat(vbcode).concat(", vorclass = ").concat(vorclass));
        }
    }
}