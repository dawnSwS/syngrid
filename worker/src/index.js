export default {
  async fetch(request, env, ctx) {
    const targetExchange = request.headers.get("x-relay-target");
    if (!targetExchange || !["binance", "okx"].includes(targetExchange)) {
      return new Response("Unauthorized Gateway Access", { status: 401 });
    }

    const url = new URL(request.url);
    const pathname = url.pathname; 
    const search = url.search;
    let targetUrl;

    const isWs = request.headers.get("Upgrade")?.toLowerCase() === "websocket";

    if (targetExchange === "binance") {
      const domain = isWs ? "nbstream.binance.com" : "eapi.binance.com";
      targetUrl = `https://${domain}${pathname}${search}`;
    } else if (targetExchange === "okx") {
      const domain = isWs ? "ws.okx.com:8443" : "www.okx.com";
      targetUrl = `https://${domain}${pathname}${search}`;
    }

    const proxyRequest = new Request(targetUrl, request);
    
    proxyRequest.headers.delete("Host");
    proxyRequest.headers.delete("Origin");
    proxyRequest.headers.delete("Referer");
    
    proxyRequest.headers.delete("x-relay-target");

    return fetch(proxyRequest);
  }
};

