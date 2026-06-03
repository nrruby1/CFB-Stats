package com.cfbstats.server.services;

import com.cfbstats.server.gen.api.DataApi;
import com.cfbstats.server.gen.model.Team;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;

@Service
public class DataHandlingClientService {

    private final DataApi dataApi;

    public DataHandlingClientService(DataApi dataApi) {
        this.dataApi = dataApi;
    }

    public Team getTeam(String school, Integer teamId, Integer year) {
        return dataApi.getTeam(school, teamId, year);
    }
}
