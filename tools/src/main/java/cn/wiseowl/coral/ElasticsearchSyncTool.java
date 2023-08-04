package cn.wiseowl.coral;

import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

/**
 * @author: haoshichuan
 * @date: 2023/8/4 10:42
 * Elasticsearch 数据同步工具
 */
public class ElasticsearchSyncTool {

    /**
     * 1. 创建客户端，用于与 Elasticsearch 集群建立连接
     * 2. 数据全量同步：
     *    --- 从一个 Elasticsearch 索引（源索引）将所有数据复制到另一个 Elasticsearch 索引（目标索引）
     *    --- 使用 Scorll API 来实现数据全量同步
     * 3. 数据增量同步：
     *    -- 将源索引中新增的数据复制到目标索引
     *    -- 通过监听 Elasticsearch 索引的变化（使用 Elasticsearch Watcher）或定期检查数据变化实现数据增量同步
     */


    private RestHighLevelClient client;

    public ElasticsearchSyncTool(String host, int port) {
        client = new RestHighLevelClient(RestClient.builder(new HttpHost(host, port, "http")));
    }

    public void closeClient() throws IOException {
        client.close();
    }

    public void fullSync(String sourceIndex, String targetIndex) throws IOException {
        Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
        SearchRequest searchRequest = new SearchRequest(sourceIndex);
        searchRequest.scroll(scroll);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        String scrollId = searchResponse.getScrollId();
        SearchHit[] searchHits = searchResponse.getHits().getHits();

        while (searchHits != null && searchHits.length > 0) {
            BulkRequest bulkRequest = new BulkRequest();
            for (SearchHit hit : searchHits) {
                IndexRequest indexRequest = new IndexRequest(targetIndex).id(hit.getId()).source(hit.getSourceAsMap());
                bulkRequest.add(indexRequest);
            }
            client.bulk(bulkRequest, RequestOptions.DEFAULT);

            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(scroll);
            searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
            scrollId = searchResponse.getScrollId();
            searchHits = searchResponse.getHits().getHits();
        }

        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
    }

    public void incrementalSync(String sourceIndex, String targetIndex, String lastSyncTime) throws IOException {
        SearchRequest searchRequest = new SearchRequest(sourceIndex);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.rangeQuery("@timestamp").gte(lastSyncTime));
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] searchHits = searchResponse.getHits().getHits();

        if (searchHits.length > 0) {
            BulkRequest bulkRequest = new BulkRequest();
            for (SearchHit hit : searchHits) {
                IndexRequest indexRequest = new IndexRequest(targetIndex).id(hit.getId()).source(hit.getSourceAsMap());
                bulkRequest.add(indexRequest);
            }
            client.bulk(bulkRequest, RequestOptions.DEFAULT);

            lastSyncTime = searchHits[searchHits.length - 1].getSourceAsMap().get("@timestamp").toString();
            System.out.println("Last sync time updated to: " + lastSyncTime);
        }
    }
}
