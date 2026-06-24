import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
    // Ramp-up otimizado para o Free Tier do Render
    stages: [
        { duration: '15s', target: 30 },  // Começa mais leve para não derrubar o cold start
        { duration: '45s', target: 50 },  // Sobe para 50 VUs (limite seguro para o Free Tier)
        { duration: '10s', target: 0 },   // Graceful shutdown
    ],
    thresholds: {
        // Tolerância de falha mantida em 5%
        http_req_failed: ['rate<0.05'], 
        // Aumentamos o p(95) para 5s, pois no Free Tier do Render 
        // a latência de I/O é alta devido ao cold start/limitação de CPU
        http_req_duration: ['p(95)<5000'], 
    },
};

export default function () {
    const url = 'https://agrisentry-iot-gateway.onrender.com/api/v1/telemetry';
    
    const payload = JSON.stringify({
        device_id: `sensor-${Math.floor(Math.random() * 1000)}`,
        sensor_type: "temperature",
        reading_value: 20 + Math.random() * 10,
        timestamp: new Date().toISOString()
    });

    const params = {
        headers: {
            'Content-Type': 'application/json',
        },
    };

    const res = http.post(url, payload, params);

    check(res, {
        'Status is 202': (r) => r.status === 202,
    });

    sleep(1); 
}