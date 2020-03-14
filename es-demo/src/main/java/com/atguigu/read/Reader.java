package com.atguigu.read;

import com.atguigu.bean.Student;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.List;

public class Reader {

    public static void main(String[] args) throws Exception{

        //todo 1.构建客户端对象工厂
        JestClientFactory factory = new JestClientFactory();

        //todo 2.设置连接参数
        HttpClientConfig config = new HttpClientConfig
                .Builder("http://hadoop102:9200").build();

        factory.setHttpClientConfig(config);

        //todo 3.从工厂获取ES客户端对象
        JestClient jestClient = factory.getObject();

        //todo 4.search创建对象
        /*Search search = new Search.Builder("{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "        \"term\": {\n" +
                "          \"class_id\": \"191010\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}")
                .addIndex("student")
                .addType("_doc")
                .build();*/

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("class_id","191010"));

        searchSourceBuilder.query(boolQueryBuilder);

        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex("student")
                .addType("_doc")
                .build();

        //todo 4.读取数据
        SearchResult result = jestClient.execute(search);

        //System.out.println(result.getTotal());

        List<SearchResult.Hit<Student, Void>> hits = result.getHits(Student.class);

        for (SearchResult.Hit<Student, Void> hit : hits) {
            System.out.println(hit.score);
        }

        jestClient.shutdownClient();

    }
}
