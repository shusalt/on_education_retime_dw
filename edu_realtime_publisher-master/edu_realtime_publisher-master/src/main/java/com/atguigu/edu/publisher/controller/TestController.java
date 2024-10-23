package com.atguigu.edu.publisher.controller;

import com.atguigu.edu.publisher.util.Mappings;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping
@Configuration
public class TestController {

    @RequestMapping("/test")
    public String getInfo() {

        System.out.println("mapping = " + Mappings.getMappings());

        return Mappings.getMappings() + "";
    }
}
