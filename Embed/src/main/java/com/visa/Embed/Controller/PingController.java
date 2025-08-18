package com.visa.Embed.Controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PingController {

    /**
     * Endpoint to test the ping functionality
     * 
     * @return pong response
     */
    @GetMapping("/ping")
    public String ping() {
        return "pong";
    }
}
