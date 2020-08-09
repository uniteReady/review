package com.tianya.log.controller;

import org.apache.log4j.Logger;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class LogController {

    public static final Logger logger = Logger.getLogger(LogController.class);


    @PostMapping(value = "/upload")

    public void upload(@RequestBody String log) {
        System.out.println(log);
        logger.info(log);
    }


}
