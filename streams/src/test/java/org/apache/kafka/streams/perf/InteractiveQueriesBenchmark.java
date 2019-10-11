/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.perf;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.net.ConnectException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import kafka.tools.StreamsResetter;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;

/**
 * Sets up a simple aggregation with interactive queries turned on. Then proceeds to query the state using simple get()s
 * and measures performance and error rates.
 */
public class InteractiveQueriesBenchmark {

    private static final String APP_ID = "streams-iq-benchmark";

    enum Command {
        DO_QUERIES, // do interactive queries against remote state
        RUN_STREAMS, // spin up the benchmark streams topology + producer
        SETUP, // cleanup stream state and recreate input topics
        ONLY_PRODUCE // simply produce the input data until termination
    }

    static class Log {

        private static final SimpleDateFormat TIMESTAMP_FMT = new SimpleDateFormat("yyyyMMddHHmmss.SSS");
        private static final boolean DEBUG_ENABLED = true;

        static void message(String msg) {
            System.out.format("[%s] %s :  %s\n" , Thread.currentThread().getName(),
                TIMESTAMP_FMT.format(new Date()), msg);
        }

        static void error(String msg, Throwable t) {
            System.err.format("[%s] %s :  %s\n" , Thread.currentThread().getName(),
                TIMESTAMP_FMT.format(new Date()), msg);
            t.printStackTrace();
        }

        static boolean isDebugEnabled() {
            return DEBUG_ENABLED;
        }
    }

    static class Workload {
        // number of users viewing pages
        static final int NUM_USERS = 1000000;
        // list of countries that come from
        static final String[] COUNTRIES = Locale.getISOCountries();
        // total number of pages in the website
        static final int NUM_PAGES = 1000;
        // Input topic
        static final String PAGEVIEWS_INPUT_TOPIC = "streams-input-pageviews";
        // Queryable state
        static final String PAGEVIEWS_AGG_STORE = "streams-iq-aggregation-store";
        // Aggregation window size
        static final int AGGREGATION_WINDOW_SECS = 60;
        // Availability measurement window
        static final int AVAILABILITY_WINDOW_MS = 60000;
        // Look back for queries
        // TODO: use this to limit query window
        private static final int QUERY_WINDOW_SECS = 600;

        // to generate random data
        final Random random = new Random();
        final Config config;

        Workload(Config config) {
            this.config = config;
        }

        /**
         * Generate key for page views input topic
         *
         * @return key of format [userId]-[countryCode]-[pageId]
         */
        String recordKey() {
            return String.format("%09d-%s-%04d", random.nextInt(NUM_USERS), COUNTRIES[random.nextInt(COUNTRIES.length)],
                random.nextInt(NUM_PAGES));
        }

        /**
         * Generate random bytes as value
         */
        byte[] recordValue() {
            int valueSize = minValueBytes() + random.nextInt(maxValueBytes() - minValueBytes());
            byte[] value = new byte[valueSize];
            random.nextBytes(value);
            return value;
        }

        /**
         * Generate key for aggregating pageviews
         *
         * @param key pageView key
         * @return key of format [pageId]-[countryCode]
         */
        static String groupKey(String key) {
            String[] splits = key.split("-");
            return splits[2] + "-" + splits[1];
        }

        /**
         * Generate a random group key to query state
         */
        String groupKey() {
            return String.format("%04d-%s", random.nextInt(NUM_PAGES), COUNTRIES[random.nextInt(COUNTRIES.length)]);
        }

        String inputTopicName() { return PAGEVIEWS_INPUT_TOPIC; }

        String stateStoreName() { return PAGEVIEWS_AGG_STORE; }

        int aggregationWindowSecs() { return AGGREGATION_WINDOW_SECS; }

        int queryWindowSecs() { return QUERY_WINDOW_SECS; }

        int minValueBytes() { return config.optionSet.valueOf(config.minValueBytesSpec); }

        int maxValueBytes() { return config.optionSet.valueOf(config.maxValueBytesSpec); }

        int produceRate() { return config.optionSet.valueOf(config.produceRateSpec); }

        int queryRate() { return config.optionSet.valueOf(config.queryRateSpec); }

        int parallelism() { return config.optionSet.valueOf(config.parallelismSpec); }

        String randomServerUri(List<String> serversUris) {
            return serversUris.get(random.nextInt(serversUris.size()));
        }

        int availabilityWindowMs() { return AVAILABILITY_WINDOW_MS; }
    }

    /**
     * Issues Remote get() against streams state & publishes metrics
     */
    static class QueryRestService extends AbstractHandler {
        private KafkaStreams streams;
        private Workload workload;
        private Server server;
        private String hostPort;
        private Client client = ClientBuilder.newBuilder().build();

        QueryRestService(KafkaStreams streams, Workload workload, Config config) {
            this.streams = streams;
            this.workload = workload;
            this.hostPort = config.optionSet.valueOf(config.serverUriSpec);
        }

        @Override
        public void handle(final String target, final Request baseRequest, final HttpServletRequest request,
            final HttpServletResponse response) throws IOException {

            if (request.getQueryString().contains("/health")) {
                Log.message("Health check : " + request.getRequestURI());
                response.setStatus(HttpServletResponse.SC_OK);
                response.getWriter().println("UP!");
            } else {
                if (Log.isDebugEnabled()) {
                    Log.message("Handling query string : " + request.getQueryString());
                }
                String key = request.getQueryString().split("=")[1];

                List<StreamsMetadata> allMetadata = streams
                    .allMetadataWithKey(workload.stateStoreName(), key, new StringSerializer());
                Log.message("Replicas :" + allMetadata.toString());

                for (final StreamsMetadata metadata : allMetadata) {
                    try {
                        if (metadata == null) {
                            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                            baseRequest.setHandled(true);
                        } else {
                            String metadataHostPort = metadata.host() + ":" + metadata.port();
                            response.setContentType("text/json; charset=utf-8");
                            if (hostPort.equals(metadataHostPort)) {
                                if (Log.isDebugEnabled()) {
                                    Log.message("Querying local store for key : " + key);
                                }
                                final ReadOnlyWindowStore<String, byte[]> windowStore = streams
                                    .store(workload.stateStoreName(),
                                        QueryableStoreTypes.windowStore());
                                String value = fetchValueAsString(windowStore, key);
                                response.setStatus(HttpServletResponse.SC_OK);
                                response.getWriter().println("{value: '" + value + "'}'");
                            } else {
                                // forward to the host, who owns the key
                                if (Log.isDebugEnabled()) {
                                    Log.message(
                                        String
                                            .format("Forwarding to remote host %s for key %s", metadataHostPort,
                                                key));
                                }
                                Response fwdResponse = client
                                    .target(String.format("http://%s/?%s", metadataHostPort, "key=" + key))
                                    .request(MediaType.APPLICATION_JSON_TYPE).get();
                                response.setStatus(fwdResponse.getStatus());
                                if (fwdResponse.getStatus() == HttpServletResponse.SC_OK) {
                                    response.getWriter()
                                        .println("{value: '" + fwdResponse.readEntity(String.class) + "'}'");
                                }
                            }
                        }
                        baseRequest.setHandled(true);
                        break;
                    } catch (Exception e) {
                        if (e.getCause() instanceof ConnectException) {
                            // failed routing
                            response.setStatus(HttpServletResponse.SC_MOVED_TEMPORARILY);
                        } else if (e instanceof InvalidStateStoreException) {
                            // invalid store
                            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                        } else {
                            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                        }
                        baseRequest.setHandled(true);
                    }
                }
            }
        }

        static String byteArrayToHex(byte[] a) {
            StringBuilder sb = new StringBuilder(a.length * 2);
            for (byte b: a)
                sb.append(String.format("%02x", b));
            return sb.toString();
        }

        String fetchValueAsString(final ReadOnlyWindowStore<String, byte[]> windowStore, final String key) {
            StringBuilder values = new StringBuilder();
            WindowStoreIterator<byte[]> iterator = windowStore.fetch(key, Instant.ofEpochMilli(0), Instant.now());
            while (iterator.hasNext()) {
                KeyValue<Long, byte[]> next = iterator.next();
                if (Log.isDebugEnabled()) {
                    Log.message("PageViews @ time " + next.key + " is " + next.value.length);
                }
                values.append(byteArrayToHex(next.value));
            }
            iterator.close();
            return values.toString();
        }

        void run() throws Exception {
            int port = Integer.parseInt(hostPort.split(":")[1]);
            server = new Server(port);
            server.setHandler(this);
            server.start();
            server.join();
        }

        public void end() throws Exception {
            if (server != null) {
                server.stop();
            }
        }
    }

    /**
     * Configurations for the benchmark
     */
    static class Config {
        OptionParser parser ;
        OptionSet optionSet;

        OptionSpec<String> commandSpec;
        OptionSpec<Integer> produceRateSpec;
        OptionSpec<Integer> minValueBytesSpec;
        OptionSpec<Integer> maxValueBytesSpec;
        OptionSpec<Integer> queryRateSpec;
        OptionSpec<Integer> parallelismSpec;
        OptionSpec<String> streamsPropFileSpec;
        OptionSpec<String> bootstrapServersSpec;
        OptionSpec<String> serverUriSpec;
        OptionSpec<String> serversUrisSpec;
        OptionSpec<String> baseDirSpec;

        Config(String[] args) {
            parser = new OptionParser(false);
            parser.accepts("help", "Print usage information.").forHelp();
            commandSpec = parser.accepts("command", "Command to run. values :"
                + Arrays.asList(Command.values())).withRequiredArg();
            produceRateSpec = parser.accepts("produce-rate", "rate to produce input at")
                .withRequiredArg().ofType(Integer.class).defaultsTo(100);
            minValueBytesSpec = parser.accepts("min-value-bytes", "minimum value size in bytes")
                .withRequiredArg().ofType(Integer.class).defaultsTo(500);
            maxValueBytesSpec = parser.accepts("max-value-bytes", "maximum value size in bytes")
                .withRequiredArg().ofType(Integer.class).defaultsTo(1024);
            queryRateSpec = parser.accepts("query-rate", "rate to query the state at")
                .withRequiredArg().ofType(Integer.class).defaultsTo(100);
            parallelismSpec = parser.accepts("parallelism", "output partitions for ")
                .withRequiredArg().ofType(Integer.class).defaultsTo(2);
            streamsPropFileSpec = parser.accepts("streams-props", "path to streams property file")
                .withRequiredArg().ofType(String.class).defaultsTo("");
            bootstrapServersSpec = parser.accepts("bootstrap-servers", "kafka bootstrap servers")
                .withRequiredArg().ofType(String.class).defaultsTo("localhost:9092");
            serverUriSpec = parser.accepts("server-uri", "host/port where queries are served off ")
                .withRequiredArg().ofType(String.class).defaultsTo("localhost:10000");
            serversUrisSpec = parser.accepts("server-uris", "uris for all server query endpoints. ")
                .withRequiredArg().withValuesSeparatedBy(',').ofType(String.class).defaultsTo("localhost:10000");
            baseDirSpec = parser.accepts("base-dir", "path to output results, metrics under")
                .withRequiredArg().ofType(String.class).defaultsTo("/tmp/iq-benchmark");
            optionSet = parser.parse(args);
        }
    }

    /**
     * Collects bunch of streams metrics into a file, in a uniform data model
     *
     */
    static class MetricsCollector implements Runnable, KafkaStreams.StateListener, StateRestoreListener {

        private final KafkaStreams streams;
        private PrintStream metricsOut;
        private Map<String, Long> restoreStartTimeMap;

        MetricsCollector(KafkaStreams streams, String serverDir) {
            this.streams = streams;
            this.restoreStartTimeMap = new HashMap<>();
            streams.setStateListener(this);
            streams.setGlobalStateRestoreListener(this);
            try {
                metricsOut = new PrintStream(new FileOutputStream(serverDir + "/metrics.out", true));
            } catch (FileNotFoundException fne) {
                Log.error("Unable to open metrics output.", fne);
            }
        }

        @Override
        public void run() {

            while (true) {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {}

                // dump all streams metrics
                String timeStamp = Log.TIMESTAMP_FMT.format(new Date());
                synchronized (metricsOut) {
                    streams.metrics().entrySet().stream().forEach(e -> {
                        String stringVal = "";
                        double doubleVal = Double.MIN_VALUE;
                        try {
                            doubleVal = Double.parseDouble(e.getValue().metricValue().toString());
                        } catch (NumberFormatException nfe) {
                            stringVal = e.getValue().metricValue().toString();
                        }

                        metricsOut.printf("|%s|%s|%s|%s|%f|%s|\n", timeStamp, e.getKey().name(), e.getKey().group(),
                            e.getKey().tags(), doubleVal, stringVal);
                    });
                    metricsOut.flush();
                }
            }
        }

        @Override
        public void onChange(final State newState, final State oldState) {
            String timeStamp = Log.TIMESTAMP_FMT.format(new Date());
            synchronized (metricsOut) {
                metricsOut.printf("|%s|%s|%s|%s|\n", timeStamp, "STATE_CHANGE", oldState.name(), newState.name());
                metricsOut.flush();
            }
        }

        @Override
        public void onRestoreStart(final TopicPartition topicPartition, final String storeName,
            final long startingOffset, final long endingOffset) {
            String timeStamp = Log.TIMESTAMP_FMT.format(new Date());
            synchronized (metricsOut) {
                restoreStartTimeMap.put(topicPartition.toString() + "#" + storeName, System.currentTimeMillis());
                metricsOut.printf("|%s|%s|%s|%d|%s|%d|%d|\n", timeStamp, "RESTORE_START", topicPartition.topic(),
                    topicPartition.partition(), storeName, startingOffset, endingOffset);
                metricsOut.flush();
            }
        }

        @Override
        public void onBatchRestored(final TopicPartition topicPartition, final String storeName,
            final long batchEndOffset, final long numRestored) {
            String timeStamp = Log.TIMESTAMP_FMT.format(new Date());
            synchronized (metricsOut) {
                metricsOut.printf("|%s|%s|%s|%d|%s|%d|%d|\n", timeStamp, "RESTORE_BATCH", topicPartition.topic(),
                    topicPartition.partition(), storeName, batchEndOffset, numRestored);
                metricsOut.flush();
            }
        }

        @Override
        public void onRestoreEnd(final TopicPartition topicPartition, final String storeName, final long totalRestored) {
            String timeStamp = Log.TIMESTAMP_FMT.format(new Date());
            synchronized (metricsOut) {
                String key = topicPartition.toString() + "#" + storeName;
                long elapsedMs = System.currentTimeMillis() - restoreStartTimeMap.remove(key);
                metricsOut.printf("|%s|%s|%s|%d|%s|%d|%d|\n", timeStamp, "RESTORE_END", topicPartition.topic(),
                    topicPartition.partition(), storeName, totalRestored, elapsedMs);
                metricsOut.flush();
            }
        }
    }

    private static void setupInputAndInternalTopics(Workload workload, String bootstrapServers) throws Exception {
        //TODO: why does setup fail to create input topic now and then
        final Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        AdminClient adminClient = AdminClient.create(props);

        // obtain the set of topics already present
        Set<String> topics = adminClient.listTopics().names().get();
        String changelogTopicName = APP_ID + "-" + workload.stateStoreName() + "-changelog";
        String repartitionTopicName = APP_ID + "-" + workload.stateStoreName() + "-repartition";

        List<String> toDelete = new ArrayList<>(Arrays.asList(workload.inputTopicName(), changelogTopicName, repartitionTopicName));
        toDelete.retainAll(topics);
        if (toDelete.size() > 0) {
            adminClient.deleteTopics(toDelete);
        }

        // wait for a bit
        Thread.sleep(5000);

        adminClient.createTopics(Arrays.asList(new NewTopic(workload.inputTopicName(), workload.parallelism(), (short) 1)));
        topics = adminClient.listTopics().names().get();
        // reset topology if needed.
        if (topics.contains(changelogTopicName) || topics.contains(repartitionTopicName)) {
            resetStreamsTopology(workload, bootstrapServers);
        }

        if (!topics.contains(workload.inputTopicName())) {
            throw new IllegalStateException("Unable to create input: " + workload.inputTopicName());
        }
    }

    private static void resetStreamsTopology(Workload workload, String bootstrapServers) {
        String changelogTopicName = APP_ID + "-" + workload.stateStoreName() + "-changelog";
        String repartitionTopicName = APP_ID + "-" + workload.stateStoreName() + "-repartition";

        final String[] parameters = new String[] {
            "--application-id", APP_ID,
            "--bootstrap-servers", bootstrapServers,
            "--to-earliest",
            "--input-topics", workload.inputTopicName(),
            "--intermediate-topics",changelogTopicName + "," + repartitionTopicName,
            "--execute"
        };
        final int exitCode = new StreamsResetter().run(parameters);
        if (exitCode != 0) {
            Log.message("Unable to reset streams topology. exit code: " + exitCode);
        }
    }

    static Properties setProduceConsumeProperties(final String clientId, final String bootStrapServers) {
        final Properties clientProps = new Properties();
        //TODO: should we do EOS and Incremental Rebalance mode = on?
        clientProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        clientProps.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        clientProps.put(ProducerConfig.LINGER_MS_CONFIG, 5000);
        clientProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 128 * 1024);
        clientProps.put(ProducerConfig.SEND_BUFFER_CONFIG, 1024 * 1024);
        clientProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        clientProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        clientProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class);
        clientProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        clientProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        clientProps.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 1024 * 1024);
        clientProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);
        return clientProps;
    }

    /**
     * Generates events into the input kafka topic at the specific rate, running as a part of the streams app.
     * TODO: Rethink. Both producer and streams will be down together, which is not how life usually is.
     */
    static void produceEvents(Workload workload, String bootstrapServers) {
        Properties producerProps = setProduceConsumeProperties("streams-iq-producer", bootstrapServers);
        RateThrottler throttler = new RateThrottler(workload.produceRate(), System.currentTimeMillis());
        long totalProduced = 0;
        long startMs = System.currentTimeMillis();
        try (final KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps)) {
            while (true) {
                producer.send(new ProducerRecord<>(workload.inputTopicName(), workload.recordKey(), workload.recordValue()));
                totalProduced++;
                if (totalProduced % 10000 == 0) {
                    long timeElapsedMs = System.currentTimeMillis() - startMs;
                    Log.message("Total produced :" + totalProduced + " in " + timeElapsedMs);
                    startMs = System.currentTimeMillis();
                }
                if (throttler.shouldThrottle(totalProduced, System.currentTimeMillis())) {
                    throttler.throttle();
                }
            }
        }
    }

    static void healthCheck(ConcurrentLinkedQueue<String> failedURIQueue, ConcurrentLinkedQueue<String> healthyURIQueue) {
        final Set<String> failedUris = new HashSet<>();
        final Client client = ClientBuilder.newBuilder().build();
        while (true) {
            try {
                // Every so often, try health checking all failed uris
                Thread.sleep(100);

                // obtain newly failed uris
                String uri;
                while ((uri = failedURIQueue.poll()) != null) {
                    failedUris.add(uri);
                }

                Response response;
                Set<String> healthyUris = new HashSet<>();
                for (String failUri : failedUris) {
                    try {
                        response = client
                            .target(String.format("http://%s/?%s", failUri, "/health"))
                            .request(MediaType.APPLICATION_JSON_TYPE).get();
                        if (response.getStatus() == HttpServletResponse.SC_OK) {
                            healthyUris.add(failUri);
                            Log.message(failUri + " is healthy again!");
                        }
                    } catch (Throwable t) { }
                }

                healthyURIQueue.addAll(healthyUris);
                failedUris.removeAll(healthyUris);
            } catch (InterruptedException e) {
                Log.error("Health check thread interrupted.", e);
            }
        }
    }

    static void doQueries(Workload workload, Config config, ConcurrentLinkedQueue<String> failedURIQueue, ConcurrentLinkedQueue<String> healthyURIQueue) {
        try {
            // Wait initially for streams thread to be ready
            Thread.sleep(2000);
            final RateThrottler throttler = new RateThrottler(workload.queryRate(), System.currentTimeMillis());
            final Client client = ClientBuilder.newBuilder().build();
            final List<String> serversUris = new ArrayList<>(config.optionSet.valuesOf(config.serversUrisSpec));

            long queriesTotal = 0;
            long failuresTotal = 0;
            long windowStartMs = System.currentTimeMillis();
            long failuresInWindow = 0;
            long queriesInWindow = 0;

            long numStoreFailuresInWindow = 0;
            long numStoreNotFoundInWindow = 0;
            long numRoutingFailsInWindow = 0;
            long numHardErrorsInWindow = 0;

            while (true) {

                // add back all the healthy uris
                String healthyUri;
                while ((healthyUri = healthyURIQueue.poll()) != null) {
                    serversUris.add(healthyUri);
                }

                // all servers are down, then wait and retry
                if (serversUris.isEmpty()) {
                    Thread.sleep(100);
                    continue;
                }

                String serverUri = workload.randomServerUri(serversUris);
                Response response = null;
                long startMs = System.currentTimeMillis();

                try {
                    if (Log.isDebugEnabled()) {
                        Log.message("Starting query to " + serverUri + "/" + workload.groupKey());
                    }
                    response = client.target(String.format("http://%s/?%s", serverUri, "key=" + workload.groupKey()))
                        .request(MediaType.APPLICATION_JSON_TYPE).get();
                    if (response.getStatus() == HttpServletResponse.SC_OK) {
                        // read and throw away value
                        response.readEntity(String.class);
                    } else {
                        if (response.getStatus() == HttpServletResponse.SC_NOT_FOUND) {
                            numStoreNotFoundInWindow++;
                        } else if (response.getStatus() == HttpServletResponse.SC_INTERNAL_SERVER_ERROR) {
                            numHardErrorsInWindow++;
                        } else if (response.getStatus() == HttpServletResponse.SC_BAD_REQUEST) {
                            numStoreFailuresInWindow++;
                        } else if (response.getStatus() == HttpServletResponse.SC_MOVED_TEMPORARILY) {
                            numRoutingFailsInWindow++;
                        }


                        failuresInWindow++;
                    }
                } catch (Throwable t) {
                    if (t.getCause() instanceof ConnectException) {
                        Log.message("Removing " + serverUri + " from routing");
                        serversUris.remove(serverUri);
                        failedURIQueue.add(serverUri);
                    }
                    failuresInWindow++;
                    t.printStackTrace();
                } finally {
                    if (Log.isDebugEnabled()) {
                        Log.message(String.format("Request to %s %s in %d ms with code %d ", serverUri,
                            (response != null && response.getStatus() == HttpServletResponse.SC_OK) ? "SUCCEEDED" : "FAILED",
                            System.currentTimeMillis() - startMs, response != null ? response.getStatus() : -1));
                    }
                }
                queriesInWindow++;
                long now = System.currentTimeMillis();
                if (throttler.shouldThrottle(queriesTotal, now)) {
                    throttler.throttle();
                }

                // output periodic metrics
                if ((now - windowStartMs) > workload.availabilityWindowMs()) {
                    windowStartMs = now;
                    queriesTotal += queriesInWindow;
                    failuresTotal += failuresInWindow;
                    Log.message(String.format("[Total] Queries %d Failures %d Success Rate: %f\n",
                        queriesTotal, failuresTotal, (100.0f * (queriesTotal - failuresTotal)) / queriesTotal));
                    Log.message(String.format("[Window] Queries %d storeFailures %d storeNotFound %d routingFails %d "
                            + " hardErrors %d Failures %d Success Rate: %f\n", queriesInWindow, numStoreFailuresInWindow,
                        numStoreNotFoundInWindow, numRoutingFailsInWindow, numHardErrorsInWindow, failuresInWindow,
                        (100.0f * (queriesInWindow - failuresInWindow)) / queriesInWindow));
                    queriesInWindow = 0;
                    failuresInWindow = 0;
                    numStoreFailuresInWindow = 0;
                    numStoreNotFoundInWindow = 0;
                    numRoutingFailsInWindow = 0;
                    numHardErrorsInWindow = 0;
                }
            }
        } catch (Throwable t) {
            Log.error("Query thread interrupted. exiting", t);
        }
    }


    public static void  main(String[] args) throws Exception {

        ExecutorService executorService = Executors.newFixedThreadPool(4);
        Config config = new Config(args);
        Workload workload = new Workload(config);
        String command = config.optionSet.valueOf(config.commandSpec);
        String bootstrapServers = config.optionSet.valueOf(config.bootstrapServersSpec);

        if (config.optionSet.has("help")) {
            config.parser.printHelpOn(System.out);
            System.exit(1);
        }

        if (command.equals(Command.SETUP.name())) {
            // recreate topics and reset the topology
            setupInputAndInternalTopics(workload, bootstrapServers);
            try {
                Files.walk(new File(config.optionSet.valueOf(config.baseDirSpec)).toPath())
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
            } catch (NoSuchFileException nfe) { }
        } else if (command.equals(Command.ONLY_PRODUCE.name())){
            // Start a producer thread, sending at the configured rate
            executorService.submit(() -> produceEvents(workload, bootstrapServers));
        } else if (command.equals(Command.DO_QUERIES.name())) {
            ConcurrentLinkedQueue<String> failedURIQueue = new ConcurrentLinkedQueue<>();
            ConcurrentLinkedQueue<String> healthyURIQueue = new ConcurrentLinkedQueue<>();
            executorService.submit(() -> doQueries(workload, config, failedURIQueue, healthyURIQueue));
            executorService.submit(() -> healthCheck(failedURIQueue, healthyURIQueue));
        } else if (command.equals(Command.RUN_STREAMS.name())) {
            // Start a producer thread, sending at the configured rate
            executorService.submit(() -> produceEvents(workload, bootstrapServers));

            // Setup Streams config
            final Properties props = new Properties();
            final String streamsPropFile = config.optionSet.valueOf(config.streamsPropFileSpec);
            if (!streamsPropFile.isEmpty()) {
                props.load(new FileReader(streamsPropFile));
            }

            // Delete/recreate output file and local state.
            String serverUri = config.optionSet.valueOf(config.serverUriSpec);
            String serverDir = config.optionSet.valueOf(config.baseDirSpec) + "/" + serverUri.replace(':', '-');
            new File(serverDir).mkdirs();

            props.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, serverUri);
            props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
            props.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "DEBUG");
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
            props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
            props.put(StreamsConfig.STATE_DIR_CONFIG, serverDir + "/state");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            // to force rebalancing asap
            //props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
            //props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 500);


            // Actual Streams benchmark DAG
            final StreamsBuilder builder = new StreamsBuilder();
            final KStream<String, byte[]> source = builder.stream(workload.inputTopicName());
            KGroupedStream<String, byte[]> groupedStream = source
                .selectKey((key, value) -> Workload.groupKey(key))
                .groupByKey();
            groupedStream.windowedBy(TimeWindows.of(Duration.ofSeconds(workload.aggregationWindowSecs())))
                .aggregate(
                    () -> new byte[0], // 0 byte initializer
                    (aggKey, newValue, aggValue) -> newValue,
                    // accept the new value, to generate some variable length updates
                    Materialized.<String, byte[], WindowStore<Bytes, byte[]>>as(workload.stateStoreName())
                        .withValueSerde(Serdes.ByteArray())
                ).toStream().print(Printed.toFile(serverDir + "/output"));

            final KafkaStreams streams = new KafkaStreams(builder.build(), props);
            final CountDownLatch latch = new CountDownLatch(1);
            QueryRestService restService = new QueryRestService(streams, workload, config);
            MetricsCollector collector = new MetricsCollector(streams, serverDir);

            // attach shutdown handler to catch control-c
            Runtime.getRuntime().addShutdownHook(new Thread("streams-iq-benchmark-shutdown-hook") {
                @Override
                public void run() {
                    try {
                        executorService.shutdownNow();
                        streams.close();
                        restService.end();
                        latch.countDown();
                    } catch (Exception e) {
                        Log.error("Unable to shutdown cleanly..", e);
                    }
                }
            });

            try {
                streams.start();
                executorService.submit(collector);
                restService.run();
                latch.await();
            } catch (final Throwable e) {
                e.printStackTrace();
                System.exit(1);
            }
        } else {
            throw new IllegalArgumentException("Unknown mode.. Please refer to --help.");
        }
    }
}
