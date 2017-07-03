//package toES;
//
//import io.searchbox.client.JestClient;
//import org.apache.log4j.Logger;
//import org.apache.logging.log4j.util.Constants;
//import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
//import org.elasticsearch.client.transport.TransportClient;
//import org.elasticsearch.common.settings.Settings;
//import org.elasticsearch.common.transport.InetSocketTransportAddress;
//import org.elasticsearch.transport.client.PreBuiltTransportClient;
//
//import java.net.InetAddress;
//import java.net.UnknownHostException;
//
//
//public class EsClient {
//    private TransportClient client;
//
//    private final static Logger LOG = Logger
//            .getLogger(EsClient.class);
//
//    public EsClient(){
//        this(Constants.ES_SERVERS.split(";"),Constants.ES_DEFAULT_PORT);
//    }
//
//
//    @SuppressWarnings("unchecked")
//    public EsClient(String[] hosts, int port){
//        Settings settings = Settings.builder()
//                .put("cluster.name", Constants.ES_CLUSTER_NAME)
//                .put("client.transport.sniff", true).build();
//
//        client = new PreBuiltTransportClient(settings);
//        for(String server : hosts){
//            try {
//                client.addTransportAddress(
//                        new InetSocketTransportAddress(InetAddress
//                                .getByName(server), port));
//            } catch (UnknownHostException e) {
//                LOG.error(e);
//                e.printStackTrace();
//            }
//        }
//    }
//
//    public  JestClient getResetClient(){
//        JestClient client = ESRestUtil.getJestClient();
//        return  client;
//    }
//
//    public EsClient(String[] hosts){
//        this(hosts,Constants.ES_DEFAULT_PORT);
//    }
//
//
//
//    public void closeClient(){
//        client.close();
//    }
//
//    public void delIndex(String index){
//        DeleteIndexResponse deleteIndexResponse = client.admin().indices()
//                .prepareDelete(index)
//                .execute().actionGet();
//    }
//
//    public TransportClient getTransportClient(){
//        return client;
//    }
//}
