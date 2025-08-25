package com.fitwhere_back.fitwhere.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SwaggerConfig {
    @Bean
    public OpenAPI openAPI() {
        return new OpenAPI()
                .info(new Info().title("FitWhere API")
                        .description("공공 체육시설 혼잡도 예측 & 예약 연동 플랫폼 API")
                        .version("v1.0.0"));
    }
}
