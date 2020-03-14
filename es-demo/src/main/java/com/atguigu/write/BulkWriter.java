package com.atguigu.write;

import com.atguigu.bean.Student;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

public class BulkWriter {

    public static void main(String[] args) throws Exception{

        //todo 1.构建客户端对象工厂
        JestClientFactory factory = new JestClientFactory();

        //todo 2.设置连接参数
        HttpClientConfig config = new HttpClientConfig
                .Builder("http://hadoop102:9200").build();

        factory.setHttpClientConfig(config);

        //todo 3.从工厂获取ES客户端对象
        JestClient jestClient = factory.getObject();

        //todo 4.数据
        Student student1 = new Student("191010", "ritian", 21, 94.4, "打撸阿撸放屁");

        Student student2 = new Student("191010", "ridi", 22, 92.4, "打撸阿撸吹牛逼");

        Student student3 = new Student("191125", "rikongqi", 23, 96.4, "打撸阿撸看动漫");

        //todo 5.创建Bulk对象
        Bulk bulk = new Bulk.Builder()
                .defaultIndex("student")
                .defaultType("_doc")
                .addAction(new Index.Builder(student1).id("1008").build())
                .addAction(new Index.Builder(student1).id("1009").build())
                .addAction(new Index.Builder(student1).id("1010").build())
                .build();

        jestClient.execute(bulk);

        jestClient.shutdownClient();

    }
}
