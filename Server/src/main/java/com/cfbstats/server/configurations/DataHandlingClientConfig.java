package com.cfbstats.server.configurations;

import com.cfbstats.server.gen.ApiClient;
import com.cfbstats.server.gen.api.DataApi;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;

@Configuration
public class DataHandlingClientConfig {

    @Bean
    ApiClient apiClient(RestClient.Builder builder) {
        ApiClient apiClient = new ApiClient(builder.baseUrl("http://127.0.0.1:8000").build());
        return apiClient;
    }

    @Bean
    DataApi dataApi(ApiClient apiClient) {
        return new DataApi(apiClient);
    }
}
