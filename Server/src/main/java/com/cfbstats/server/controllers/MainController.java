package com.cfbstats.server.controllers;

import com.cfbstats.server.gen.model.Team;
import com.cfbstats.server.services.DataHandlingClientService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestClientException;

@Controller
@RequestMapping(path="/")
public class MainController {

    @Autowired
    private DataHandlingClientService dataHandlingClientService;

    @GetMapping(path="/hello")
    public @ResponseBody String hello() {
        return "Hello World";
    }

    @GetMapping(path="/team")
    public @ResponseBody Team get_team(@RequestParam(required = false) String school, @RequestParam(required = false) Integer teamId, @RequestParam(required = false) Integer year) {
        if (school == null && teamId == null) {
            throw new IllegalArgumentException("At least one of 'school' or 'teamId' must be provided");
        }
        return dataHandlingClientService.getTeam(school, teamId, year);
    }
}
