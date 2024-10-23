package com.atguigu.edu.publisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Map;

@SpringBootApplication
@MapperScan(basePackages = "com.atguigu.edu.publisher.mapper")
public class EduRealtimePublisher {

    public static void main(String[] args) {
        SpringApplication.run(EduRealtimePublisher.class, args);
    }

}
