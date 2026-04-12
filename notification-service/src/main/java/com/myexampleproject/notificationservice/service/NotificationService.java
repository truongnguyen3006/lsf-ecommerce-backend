package com.myexampleproject.notificationservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;


@Service
@RequiredArgsConstructor
@Slf4j
public class NotificationService {

    private final SimpMessagingTemplate messagingTemplate;
    private final ObjectMapper objectMapper;

    private String cleanJson(String raw) {
        if (raw == null) {
            return "";
        }
        int jsonStart = raw.indexOf('{');
        return jsonStart >= 0 ? raw.substring(jsonStart) : raw;
    }
}
