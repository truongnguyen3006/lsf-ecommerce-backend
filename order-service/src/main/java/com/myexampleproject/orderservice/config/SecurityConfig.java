package com.myexampleproject.orderservice.config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.web.SecurityFilterChain;

@Configuration
@EnableWebSecurity
public class SecurityConfig {

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http
                // 1. Tắt CSRF (vì dùng API)
                .csrf(csrf -> csrf.disable())

                // 2. Yêu cầu TẤT CẢ request phải được xác thực
                .authorizeHttpRequests(auth -> auth
                        .requestMatchers("/actuator/**").permitAll()
                        .requestMatchers("/v3/api-docs/**").permitAll()
                        .requestMatchers("/swagger-ui/**").permitAll()
                        .requestMatchers("/swagger-ui.html").permitAll()

                                .requestMatchers("/lsf/outbox").hasRole("ADMIN")
                        .requestMatchers("/lsf/outbox/**").hasRole("ADMIN")
                        .requestMatchers("/lsf/kafka").hasRole("ADMIN")
                        .requestMatchers("/lsf/kafka/**").hasRole("ADMIN")
//                                .requestMatchers("/lsf/outbox/**").authenticated()
                                .requestMatchers("/admin/outbox").hasRole("ADMIN")
                                .requestMatchers("/admin/saga").hasRole("ADMIN")
                                .requestMatchers("/admin/saga/**").hasRole("ADMIN")
                                .requestMatchers("/admin/kafka").hasRole("ADMIN")
                                .requestMatchers("/admin/kafka/**").hasRole("ADMIN")
                                .requestMatchers("/admin/outbox/**").hasRole("ADMIN")
//                                .requestMatchers("/admin/outbox/**").authenticated()
                        .requestMatchers(HttpMethod.GET, "/api/order/**").permitAll()
                        .requestMatchers(HttpMethod.POST, "/api/order").authenticated()
                        .anyRequest().authenticated()
                )

                .oauth2ResourceServer(oauth2 -> oauth2
                        .jwt(jwt -> jwt.jwtAuthenticationConverter(jwtAuthenticationConverter()))
                )
                // 4. Bắt buộc stateless (không dùng session)
                .sessionManagement(session -> session
                        .sessionCreationPolicy(SessionCreationPolicy.STATELESS));

        return http.build();
    }

    private JwtAuthenticationConverter jwtAuthenticationConverter() {
        JwtAuthenticationConverter converter = new JwtAuthenticationConverter();
        converter.setJwtGrantedAuthoritiesConverter(new KeycloakJwtRoleConverter());
        return converter;
    }
}
