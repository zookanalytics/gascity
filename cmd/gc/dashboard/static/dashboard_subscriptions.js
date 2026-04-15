export function createDashboardSubscriptions(options) {
    var state = options.state;

    function subscribeEvents() {
        var payload = {kind: 'events'};
        if (state.lastEventCursor) {
            if (state.selectedCity) {
                payload.after_seq = parseInt(state.lastEventCursor, 10) || 0;
            } else {
                payload.after_cursor = state.lastEventCursor;
            }
        }
        options.wsRequest('subscription.start', payload).then(function() {
            state.subscriptionRetry = 0;
        }).catch(function(err) {
            options.handleError(err, 'subscription.start');
            if (state.subscriptionRetry++ < 3) {
                var delay = Math.min(1000 * Math.pow(2, state.subscriptionRetry), 10000);
                setTimeout(subscribeEvents, delay);
            }
        });
    }

    function handleWSEvent(msg) {
        if (msg.cursor) {
            state.lastEventCursor = msg.cursor;
        } else if (msg.index) {
            state.lastEventCursor = String(msg.index);
        }

        if (window.pauseRefresh) {
            return;
        }
        var eventType = msg.event_type || '';

        scheduleActivityRefresh();
        if (eventType && state.observationTypes[eventType]) {
            return;
        }
        scheduleFullRefresh();
    }

    function scheduleActivityRefresh() {
        if (state.activityTimer) {
            return;
        }
        state.activityTimer = setTimeout(function() {
            state.activityTimer = null;
            if (window.pauseRefresh) {
                return;
            }
            options.wsRequest('events.list', {limit: 50}).then(function(data) {
                options.renderActivityPanel(data.items || []);
            }).catch(function(err) {
                options.handleError(err, 'activity.refresh');
            });
        }, state.activityThrottle);
    }

    function scheduleFullRefresh() {
        if (state.fullRefreshTimer) {
            clearTimeout(state.fullRefreshTimer);
        }
        state.fullRefreshTimer = setTimeout(function() {
            state.fullRefreshTimer = null;
            if (state.activityTimer) {
                clearTimeout(state.activityTimer);
                state.activityTimer = null;
            }
            if (window.pauseRefresh) {
                return;
            }
            options.loadDashboard();
        }, 500);
    }

    return {
        handleWSEvent: handleWSEvent,
        subscribeEvents: subscribeEvents
    };
}
