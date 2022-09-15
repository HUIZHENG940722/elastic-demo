package com.ethan.course.elastic;

import com.alibaba.fastjson.JSON;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

@SpringBootTest
public class ElasticRootApplicationTests {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    /**
     * 测试索引的创建
     */
    @Test
    public void testCreateIndex() throws IOException {
        // 1、创建索引请求
        CreateIndexRequest request = new CreateIndexRequest("ethan_index");
        // 2、执行客户端执行请求
        CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse);
    }

    /**
     * 测试获取索引，判断索引是否存在
     *
     * @throws IOException
     */
    @Test
    public void testExistIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("ethan_index");
        boolean exists = restHighLevelClient.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    /**
     * 删除索引测试
     *
     * @throws IOException
     */
    @Test
    public void testDeleteIndex() throws IOException {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("ethan_index");
        AcknowledgedResponse delete = restHighLevelClient.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        System.out.println(delete);
    }

    /**
     * 测试添加文档
     */
    @Test
    public void testAddDocument() throws IOException {
        // 1、创建对象
        User user = new User("郑辉", 28);
        // 2、创建请求
        IndexRequest request = new IndexRequest("ethan");
        // 3、规则put /ethan/_doc/1
        request.id("1");
        request.timeout(TimeValue.timeValueSeconds(1));
        request.timeout("1s");
        // 4、将我们的数据放入请求Json
        request.source(JSON.toJSONString(user), XContentType.JSON);
        // 5、客户端发起请求
        IndexResponse index = restHighLevelClient.index(request, RequestOptions.DEFAULT);
        System.out.println(index.toString());
        System.out.println(index.status());
    }

    /**
     * 测试文档是否存在
     *
     * @throws IOException
     */
    @Test
    public void testExists() throws IOException {
        GetRequest getRequest = new GetRequest("ethan", "1");
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");
        boolean exists = restHighLevelClient.exists(getRequest, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    /**
     * 测试文档
     */
    @Test
    public void testGetDocument() throws IOException {
        GetRequest getRequest = new GetRequest("ethan", "1");
        GetResponse get = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        System.out.println(get.getSourceAsString());
        System.out.println(get);
    }

    /**
     * 更新文档记录
     *
     * @throws IOException
     */
    @Test
    public void testUpdateDocument() throws IOException {
        UpdateRequest update = new UpdateRequest("ethan", "1");
        update.timeout("1s");
        User user = new User("郑辉", 28);
        UpdateRequest doc = update.doc(JSON.toJSONString(user), XContentType.JSON);
        UpdateResponse response = restHighLevelClient.update(doc, RequestOptions.DEFAULT);
        System.out.println(response.status());
    }

    /**
     * 删除文档记录
     */
    @Test
    public void testDeleteDocument() throws IOException {
        DeleteRequest request = new DeleteRequest("ethan", "1");
        request.timeout("1s");
        DeleteResponse delete = restHighLevelClient.delete(request, RequestOptions.DEFAULT);
    }

    /**
     * 特殊的，真的项目一般都会批量插入数据
     */
    @Test
    public void testRequest() throws IOException {
        BulkRequest request = new BulkRequest();
        request.timeout("10s");
        ArrayList<User> userList = new ArrayList<>();
        userList.add(new User("ethan1", 12));
        userList.add(new User("ethan2", 28));
        userList.add(new User("ethan3", 30));
        userList.add(new User("ethan4", 34));
        userList.add(new User("ethan5", 38));
        for (int i = 0; i < userList.size(); i++) {
            request
                .add(new IndexRequest("ethan")
                    .id(""+(i+1)).source(JSON.toJSONString(userList.get(i)), XContentType.JSON));

        }
        BulkResponse bulk = restHighLevelClient.bulk(request, RequestOptions.DEFAULT);
        System.out.println(bulk.hasFailures());
    }

    // 查询
    // SearchRequest 搜索请求
    // Search
    @Test
    public void testSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("ethan");
        // 1、构建搜索条件
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 2、查询条件，我们可以使用QueryBuilders工具来实现
        // QueryBuilders.termQuery精确
        // QueryBuilders.matchAllQuery()配置所有
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "ethan1");
        searchSourceBuilder.query(termQueryBuilder);
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        searchRequest.source(searchSourceBuilder);
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(search.getHits());
    }
}
