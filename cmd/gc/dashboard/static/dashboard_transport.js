export function createDashboardTransport(options) {
    var state = options.state;

    function failPendingRequests(err) {
        var ids = Object.keys(state.wsPending);
        for (var i = 0; i < ids.length; i++) {
            var id = ids[i];
            if (state.wsPending[id]) {
                state.wsPending[id].reject(err);
                delete state.wsPending[id];
            }
        }
    }

    function wsRequest(action, payload) {
        return new Promise(function(resolve, reject) {
            if (!state.ws || state.ws.readyState !== WebSocket.OPEN) {
                reject(new Error('WebSocket not connected'));
                return;
            }
            state.wsReqId++;
            var id = 'dash-' + state.wsReqId;
            var msg = {type: 'request', id: id, action: action};
            if (state.selectedCity) {
                msg.scope = {city: state.selectedCity};
            }
            if (payload) {
                msg.payload = payload;
            }
            state.wsPending[id] = {resolve: resolve, reject: reject};
            state.ws.send(JSON.stringify(msg));
            setTimeout(function() {
                if (state.wsPending[id]) {
                    state.wsPending[id].reject(new Error('WebSocket request timeout'));
                    delete state.wsPending[id];
                }
            }, 30000);
        });
    }

    function resolveDefaultCity(hello) {
        if (state.selectedCity) {
            return Promise.resolve();
        }
        if (!hello || hello.server_role !== 'supervisor') {
            return Promise.resolve();
        }
        return wsRequest('cities.list').then(function(data) {
            var items = (data && data.items) || [];
            state.citiesList = items;
            if (items.length > 0 && items[0].name) {
                state.selectedCity = items[0].name;
            }
            options.updateCityTabs(items);
        }).catch(function(err) {
            options.handleError(err, 'cities.list.default');
        });
    }

    function connectWebSocket() {
        if (!state.wsUrl) {
            options.updateConnectionStatus('error');
            options.handleError(new Error('dashboard bootstrap missing apiBaseURL'), 'bootstrap');
            return;
        }
        if (state.ws) {
            state.ws.close();
        }

        var socket = new WebSocket(state.wsUrl);
        state.ws = socket;

        socket.onopen = function() {
            // Hello envelope arrives as the first message.
        };

        socket.onmessage = function(e) {
            if (state.ws !== socket) {
                return;
            }
            if (typeof e.data !== 'string') {
                options.handleError(new Error('received non-text WebSocket frame'), 'ws.frame');
                socket.close();
                return;
            }

            var msg;
            try {
                msg = JSON.parse(e.data);
            } catch (err) {
                options.handleError(err, 'ws.message');
                socket.close();
                return;
            }

            switch (msg.type) {
            case 'hello':
                window.wsConnected = true;
                state.wsReconnectDelay = 1000;
                state.subscriptionRetry = 0;
                options.updateConnectionStatus('live');
                options.clearError();
                Promise.resolve(options.onHello(msg)).catch(function(err) {
                    options.handleError(err, 'ws.hello');
                });
                break;
            case 'response':
                if (msg.id && state.wsPending[msg.id]) {
                    state.wsPending[msg.id].resolve(msg.result);
                    delete state.wsPending[msg.id];
                }
                break;
            case 'error':
                if (msg.id && state.wsPending[msg.id]) {
                    state.wsPending[msg.id].reject(new Error(msg.message || msg.code || 'API error'));
                    delete state.wsPending[msg.id];
                } else {
                    options.handleError(new Error(msg.message || msg.code || 'API error'), 'ws.error-envelope');
                }
                break;
            case 'event':
                options.onEvent(msg);
                break;
            default:
                options.handleError(new Error('unknown WebSocket message type: ' + msg.type), 'ws.message-type');
                break;
            }
        };

        socket.onclose = function() {
            if (state.ws === socket) {
                state.ws = null;
            }
            window.wsConnected = false;
            failPendingRequests(new Error('WebSocket connection closed'));
            options.updateConnectionStatus('reconnecting');
            setTimeout(function() {
                state.wsReconnectDelay = Math.min(state.wsReconnectDelay * 2, state.wsMaxReconnectDelay);
                connectWebSocket();
            }, state.wsReconnectDelay);
        };

        socket.onerror = function() {
            if (state.ws !== socket) {
                return;
            }
            options.handleError(new Error('WebSocket transport error'), 'ws.error');
        };
    }

    return {
        connectWebSocket: connectWebSocket,
        resolveDefaultCity: resolveDefaultCity,
        wsRequest: wsRequest
    };
}
