// Cloudflare Pages Function – Streaming AI Chat API
export async function onRequest(context) {
    const { request, env } = context;

    // CORS preflight
    if (request.method === 'OPTIONS') {
        return new Response(null, {
            headers: {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'POST, OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type, Authorization',
            },
        });
    }

    if (request.method !== 'POST') {
        return new Response('Method not allowed', { status: 405 });
    }

    // Authentication
    const authHeader = request.headers.get('Authorization');
    const expectedKey = env.API_KEY;
    if (!expectedKey || authHeader !== `Bearer ${expectedKey}`) {
        return new Response(JSON.stringify({ error: 'Unauthorized' }), {
            status: 401,
            headers: { 'Content-Type': 'application/json' },
        });
    }

    try {
        const { prompt, model = '@cf/meta/llama-3.3-70b-instruct', stream = true } = await request.json();
        if (!prompt) {
            return new Response(JSON.stringify({ error: 'Missing prompt' }), { status: 400 });
        }

        // If streaming is requested, use SSE
        if (stream) {
            const aiStream = await env.AI.run(model, { prompt, stream: true });
            // Transform the AI stream into SSE format
            const encoder = new TextEncoder();
            const readable = new ReadableStream({
                async start(controller) {
                    const reader = aiStream.getReader();
                    try {
                        while (true) {
                            const { done, value } = await reader.read();
                            if (done) break;
                            // Each chunk is a string token
                            const chunk = `data: ${JSON.stringify({ token: value })}\n\n`;
                            controller.enqueue(encoder.encode(chunk));
                        }
                        controller.enqueue(encoder.encode('data: [DONE]\n\n'));
                        controller.close();
                    } catch (err) {
                        controller.error(err);
                    }
                },
            });

            return new Response(readable, {
                headers: {
                    'Content-Type': 'text/event-stream',
                    'Cache-Control': 'no-cache',
                    'Connection': 'keep-alive',
                    'Access-Control-Allow-Origin': '*',
                },
            });
        } else {
            // Non‑streaming fallback
            const response = await env.AI.run(model, { prompt });
            return new Response(JSON.stringify(response), {
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            });
        }
    } catch (err) {
        return new Response(JSON.stringify({ error: err.message }), {
            status: 500,
            headers: { 'Content-Type': 'application/json' },
        });
    }
}
