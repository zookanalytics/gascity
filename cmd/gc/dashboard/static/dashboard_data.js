export function createDashboardData(options) {
    var state = options.state;

    function requestWithContext(action, payload, context) {
        return options.wsRequest(action, payload).catch(function(err) {
            err.dashboardContext = context;
            throw err;
        });
    }

    function loadDashboard() {
        return Promise.all([
            requestWithContext('convoys.list', null, 'loadDashboard.convoys'),
            requestWithContext('sessions.list', {state: 'active', peek: true}, 'loadDashboard.sessions'),
            requestWithContext('mail.list', null, 'loadDashboard.mail'),
            requestWithContext('beads.list', {status: 'open'}, 'loadDashboard.beadsOpen'),
            requestWithContext('beads.list', {status: 'in_progress'}, 'loadDashboard.beadsIP'),
            requestWithContext('events.list', {limit: 50}, 'loadDashboard.events'),
            requestWithContext('status.get', null, 'loadDashboard.status'),
            requestWithContext('rigs.list', null, 'loadDashboard.rigs'),
            requestWithContext('services.list', null, 'loadDashboard.services'),
            requestWithContext('agents.list', null, 'loadDashboard.agents')
        ]).then(function(results) {
            var convoys = results[0].items || [];
            var sessions = results[1].items || [];
            var mail = results[2].items || [];
            var beadsOpen = results[3].items || [];
            var beadsInProgress = results[4].items || [];
            var events = results[5].items || [];
            var rigs = results[7].items || [];
            var services = results[8].items || [];
            var agents = results[9].items || [];

            var allBeads = beadsOpen.concat(beadsInProgress);

            var escalations = beadsOpen.filter(function(b) {
                var labels = b.labels || [];
                return labels.indexOf('gc:escalation') !== -1;
            }).map(function(b) {
                var severity = 'medium';
                var acked = false;
                (b.labels || []).forEach(function(l) {
                    if (l.indexOf('severity:') === 0) {
                        severity = l.replace('severity:', '');
                    }
                    if (l === 'acked') {
                        acked = true;
                    }
                });
                return {
                    id: b.id,
                    title: b.title,
                    severity: severity,
                    acked: acked,
                    escalated_by: options.formatAgentAddress(b.from || ''),
                    created_at: b.created_at,
                    from: b.from
                };
            });

            var sevOrder = {critical: 0, high: 1, medium: 2, low: 3};
            escalations.sort(function(a, b) {
                return (sevOrder[a.severity] || 3) - (sevOrder[b.severity] || 3);
            });

            var assigned = beadsInProgress.filter(function(b) { return b.assignee; });

            options.renderConvoysPanel(convoys);
            options.renderCrewPanel(sessions);
            options.renderPolecatsPanel(sessions);
            options.renderActivityPanel(events);
            options.renderMailAllPanel(mail);
            options.renderEscalationsPanel(escalations);
            options.renderServicesPanel(services);
            options.renderRigsPanel(rigs, sessions, agents);
            options.renderDogsPanel(sessions);
            options.renderQueuesPanel(allBeads);
            options.renderBeadsPanel(allBeads, rigs);
            options.renderAssignedPanel(assigned);
            options.updateMayorBanner(sessions);
            options.updateSummaryBanner({
                sessions: sessions,
                beadsOpen: beadsOpen,
                beadsInProgress: beadsInProgress,
                convoys: convoys,
                escalations: escalations
            });
            options.updateCityTabs(state.citiesList);
            options.clearError();
            loadMailInbox(mail);
        }).catch(function(err) {
            options.handleError(err, err.dashboardContext || 'loadDashboard');
        });
    }

    function loadMailInbox(mailData) {
        var loading = document.getElementById('mail-loading');
        var threadsContainer = document.getElementById('mail-threads');
        var empty = document.getElementById('mail-empty');
        var count = document.getElementById('mail-count');

        if (!threadsContainer) {
            return;
        }

        var doRender = function(messages) {
            if (loading) {
                loading.style.display = 'none';
            }
            var threads = options.groupMailIntoThreads(messages);

            if (threads.length === 0) {
                threadsContainer.style.display = 'none';
                if (empty) {
                    empty.style.display = 'block';
                }
                if (count) {
                    count.textContent = '0';
                }
                return;
            }

            threadsContainer.style.display = 'block';
            if (empty) {
                empty.style.display = 'none';
            }

            var unreadTotal = 0;
            threads.forEach(function(thread) { unreadTotal += thread.unread_count; });
            if (count) {
                count.textContent = unreadTotal > 0 ? unreadTotal + ' unread' : String(threads.length);
                if (unreadTotal > 0) {
                    count.classList.add('has-unread');
                } else {
                    count.classList.remove('has-unread');
                }
            }

            var html = '';
            for (var i = 0; i < threads.length; i++) {
                var thread = threads[i];
                var last = thread.last_message || {};
                var hasMultiple = thread.count > 1;
                var unreadClass = thread.unread_count > 0 ? ' mail-thread-unread' : '';
                var countBadge = hasMultiple ? '<span class="thread-count">' + thread.count + '</span>' : '';
                var unreadDot = thread.unread_count > 0 ? '<span class="thread-unread-dot"></span>' : '';

                var priorityIcon = '';
                if (last.priority === 0 || last.priority === 'urgent') {
                    priorityIcon = '<span class="priority-urgent">⚡</span> ';
                } else if (last.priority === 1 || last.priority === 'high') {
                    priorityIcon = '<span class="priority-high">!</span> ';
                }

                var timeStr = last.created_at ? options.formatTimestamp(last.created_at) : '';
                var relativeStr = last.created_at ? ' (' + options.formatAge(last.created_at) + ')' : '';

                html += '<div class="mail-thread' + unreadClass + '">' +
                    '<div class="mail-thread-header" data-thread-id="' + options.escapeHtml(thread.thread_id) + '"' +
                    (hasMultiple ? '' : ' data-msg-id="' + options.escapeHtml(last.id || '') + '" data-from="' + options.escapeHtml(last.from || '') + '"') + '>' +
                    '<div class="mail-thread-left">' + unreadDot +
                    '<span class="mail-from">' + options.escapeHtml(options.formatAgentAddress(last.from)) + '</span>' + countBadge +
                    '</div>' +
                    '<div class="mail-thread-center">' + priorityIcon +
                    '<span class="mail-subject">' + options.escapeHtml(thread.subject || '') + '</span>' +
                    (hasMultiple && last.body ? '<span class="mail-thread-preview"> — ' + options.escapeHtml(last.body.substring(0, 60)) + '</span>' : '') +
                    '</div>' +
                    '<div class="mail-thread-right"><span class="mail-time">' + options.escapeHtml(timeStr + relativeStr) + '</span></div>' +
                    '</div>';

                if (hasMultiple) {
                    html += '<div class="mail-thread-messages" style="display: none;">';
                    for (var j = 0; j < thread.messages.length; j++) {
                        var msg = thread.messages[j];
                        var msgUnread = msg.read ? '' : ' mail-unread';
                        html += '<div class="mail-thread-msg' + msgUnread + '" data-msg-id="' + options.escapeHtml(msg.id) + '" data-from="' + options.escapeHtml(msg.from || '') + '">' +
                            '<div class="mail-thread-msg-header">' +
                            '<span class="mail-from">' + options.escapeHtml(options.formatAgentAddress(msg.from)) + '</span>' +
                            '<span class="mail-time">' + options.escapeHtml(msg.created_at ? options.formatTimestamp(msg.created_at) : '') + '</span>' +
                            '</div>' +
                            '<div class="mail-thread-msg-subject">' + options.escapeHtml(msg.subject || '') + '</div>' +
                            '</div>';
                    }
                    html += '</div>';
                }

                html += '</div>';
            }
            threadsContainer.innerHTML = html;
        };

        if (mailData) {
            doRender(mailData);
            return;
        }

        options.wsRequest('mail.list').then(function(data) {
            doRender(data.items || []);
        }).catch(function(err) {
            if (loading) {
                loading.textContent = 'Failed to load mail';
            }
            options.handleError(err, 'loadMailInbox');
        });
    }

    return {
        loadDashboard: loadDashboard,
        loadMailInbox: loadMailInbox
    };
}
