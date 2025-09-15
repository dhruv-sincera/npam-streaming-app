package com.sincera.npam.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class UiController {
    @GetMapping("/npam-ui")
    public String home() {
        return "dashboard";
    }
}
