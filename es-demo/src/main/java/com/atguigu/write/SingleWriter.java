package com.atguigu.write;

import com.atguigu.bean.Student;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

public class SingleWriter {

    public static void main(String[] args) throws Exception{

        //todo 1.构建客户端对象工厂
        JestClientFactory factory = new JestClientFactory();

        //todo 2.设置连接参数
        HttpClientConfig config = new HttpClientConfig
                .Builder("http://hadoop102:9200").build();

        factory.setHttpClientConfig(config);

        //todo 3.从工厂获取ES客户端对象
        JestClient jestClient = factory.getObject();

        //todo 4.准备数据
        Student student = new Student(
                "191010",
                "liqing",
                20,
                92.4,
                "踢球"
        );

        Index index = new Index.Builder(student)
                .index("student")
                .type("_doc")
                .id("1007")
                .build();

        /*Index index = new Index.Builder("{\n" +
                "  \"class_id\":\"191010\",\n" +
                "  \"name\":\"Hasaki\",\n" +
                "  \"age\":22,\n" +
                "  \"score\":89.0,\n" +
                "  \"hobby\":\"吃肯德基打橄榄球\"\n" +
                "}")
                .index("student")
                .type("_doc")
                .id("1006")
                .build();*/

        //todo 5.写入数据
        jestClient.execute(index);

        //todo 6.关闭连接
        jestClient.shutdownClient();
    }
}
