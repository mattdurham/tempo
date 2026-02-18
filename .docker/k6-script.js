import { check, sleep } from 'k6';
import { Rate, Trend, Counter } from 'k6/metrics';
import http from 'k6/http';

const tracesSent = new Counter('traces_sent');
const traceDuration = new Trend('trace_duration_ms');
const errorRate = new Rate('errors');

// Helper to generate OTLP trace data
function generateOTLPTrace() {
  const traceID = Math.random().toString(16).substring(2, 34).padEnd(32, '0');
  const spanID = Math.random().toString(16).substring(2, 18).padEnd(16, '0');
  const now = Date.now();
  const nanoMultiplier = 1000000;

  return {
    resourceSpans: [
      {
        resource: {
          attributes: [
            {
              key: 'service.name',
              value: { stringValue: 'k6-load-tester' },
            },
            {
              key: 'service.version',
              value: { stringValue: '1.0.0' },
            },
          ],
        },
        scopeSpans: [
          {
            scope: {
              name: 'k6',
              version: '1.0.0',
            },
            spans: [
              {
                traceId: traceID,
                spanId: spanID,
                name: `request-${Math.random().toString(36).substring(7)}`,
                kind: 1,
                startTimeUnixNano: (now - 100) * nanoMultiplier,
                endTimeUnixNano: now * nanoMultiplier,
                attributes: [
                  { key: 'http.method', value: { stringValue: 'GET' } },
                  { key: 'http.url', value: { stringValue: '/api/traces' } },
                  { key: 'http.status_code', value: { intValue: 200 } },
                ],
              },
              {
                traceId: traceID,
                spanId: Math.random().toString(16).substring(2, 18).padEnd(16, '0'),
                parentSpanId: spanID,
                name: 'db.query',
                kind: 3,
                startTimeUnixNano: (now - 50) * nanoMultiplier,
                endTimeUnixNano: (now - 10) * nanoMultiplier,
                attributes: [
                  { key: 'db.system', value: { stringValue: 'postgresql' } },
                  { key: 'db.operation', value: { stringValue: 'SELECT' } },
                  { key: 'db.rows', value: { intValue: Math.floor(Math.random() * 1000) } },
                ],
              },
            ],
          },
        ],
      },
    ],
  };
}

export const options = {
  stages: [
    { duration: '10s', target: parseInt(__ENV.VUS || '2') },
    { duration: __ENV.DURATION || '60s', target: parseInt(__ENV.VUS || '2') },
    { duration: '10s', target: 0 },
  ],
  thresholds: {
    'errors': ['rate<0.1'],
    'traces_sent': ['count>0'],
  },
};

export default function () {
  const endpoint = __ENV.TEMPO_ENDPOINT || 'http://localhost:4318';
  const traceRate = parseInt(__ENV.TRACE_RATE || '10');

  for (let i = 0; i < traceRate; i++) {
    const trace = generateOTLPTrace();
    const startTime = Date.now();

    // Retry logic with exponential backoff for initial connection failures
    let res = null;
    let retries = 0;
    const maxRetries = 3;

    while (retries < maxRetries && !res) {
      try {
        res = http.post(`${endpoint}/v1/traces`, JSON.stringify(trace), {
          headers: {
            'Content-Type': 'application/json',
          },
          timeout: '30s',
        });
      } catch (error) {
        retries++;
        if (retries < maxRetries) {
          console.log(`Connection attempt ${retries} failed, retrying in ${Math.pow(2, retries)}s...`);
          sleep(Math.pow(2, retries));
        } else {
          console.log(`Failed to connect after ${maxRetries} retries: ${error}`);
          errorRate.add(1);
          continue;
        }
      }
    }

    if (!res) continue;

    const duration = Date.now() - startTime;
    traceDuration.add(duration);

    const success = check(res, {
      'status 200 or 204': (r) => r.status === 200 || r.status === 204,
      'response time < 1s': (r) => r.timings.duration < 1000,
    });

    if (success) {
      tracesSent.add(1);
    } else {
      errorRate.add(1);
      console.log(`Error: ${res.status} - ${res.body}`);
    }
  }

  sleep(1);
}
