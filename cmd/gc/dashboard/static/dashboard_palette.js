export function createCommandPalette(options) {
    var state = options.state;
    var allCommands = [];
    var visibleCommands = [];
    var selectedIdx = 0;
    var isPaletteOpen = false;
    var executionLock = false;
    var pendingCommand = null;
    var cachedOptions = null;
    var recentCommands = [];
    var MAX_RECENT = 10;
    var RECENT_STORAGE_KEY = 'gt-palette-recent';

    var overlay = document.getElementById('command-palette-overlay');
    var searchInput = document.getElementById('command-palette-input');
    var resultsDiv = document.getElementById('command-palette-results');
    var outputPanel = document.getElementById('output-panel');
    var outputContent = document.getElementById('output-panel-content');
    var outputCmd = document.getElementById('output-panel-cmd');

    function loadRecentCommands() {
        try {
            var stored = localStorage.getItem(RECENT_STORAGE_KEY);
            if (stored) {
                recentCommands = JSON.parse(stored);
                if (!Array.isArray(recentCommands)) {
                    recentCommands = [];
                }
                recentCommands = recentCommands.slice(0, MAX_RECENT);
            }
        } catch (err) {
            recentCommands = [];
            options.handleError(err, 'palette.localStorage.load');
        }
    }

    function saveRecentCommand(cmdName) {
        recentCommands = recentCommands.filter(function(c) { return c !== cmdName; });
        recentCommands.unshift(cmdName);
        recentCommands = recentCommands.slice(0, MAX_RECENT);
        try {
            localStorage.setItem(RECENT_STORAGE_KEY, JSON.stringify(recentCommands));
        } catch (err) {
            options.handleError(err, 'palette.localStorage.save');
        }
    }

    function buildCommandsFromCapabilities(caps) {
        state.wsCapabilities = caps || [];
        allCommands = [
            {name: 'status', desc: 'Show city status', category: 'System'},
            {name: 'mail inbox', desc: 'Show mail inbox', category: 'Mail'},
            {name: 'mail send', desc: 'Send mail', category: 'Mail', args: '<address> -s <subject> -m <message>', argType: 'agents'},
            {name: 'mail read', desc: 'Read a message', category: 'Mail', args: '<id>', argType: 'messages'},
            {name: 'mail archive', desc: 'Archive a message', category: 'Mail', args: '<id>', argType: 'messages'},
            {name: 'convoy list', desc: 'List convoys', category: 'Work'},
            {name: 'convoy create', desc: 'Create convoy', category: 'Work', args: '<name>'},
            {name: 'convoy status', desc: 'Show convoy detail', category: 'Work', args: '<id>', argType: 'convoys'},
            {name: 'rig list', desc: 'List rigs', category: 'System'},
            {name: 'agent list', desc: 'List agents', category: 'System'},
            {name: 'sling', desc: 'Sling a bead to a rig', category: 'Work', args: '<bead_id> <rig>', argType: 'hooks'},
            {name: 'unsling', desc: 'Unassign a bead', category: 'Work', args: '<bead_id>', argType: 'hooks'}
        ];

        if (state.wsCapabilities.indexOf('config.get') !== -1) {
            allCommands.push({name: 'config get', desc: 'Show city config', category: 'System'});
        }
        if (state.wsCapabilities.indexOf('config.validate') !== -1) {
            allCommands.push({name: 'config validate', desc: 'Validate city config', category: 'System'});
        }
    }

    function fetchOptions() {
        return Promise.all([
            options.wsRequest('rigs.list'),
            options.wsRequest('sessions.list', {state: 'active'}),
            options.wsRequest('beads.list', {status: 'open'}),
            options.wsRequest('mail.list')
        ]).then(function(results) {
            cachedOptions = {
                rigs: (results[0].items || []).map(function(rig) { return rig.name || ''; }).filter(Boolean),
                agents: (results[1].items || []).map(function(sessionItem) { return sessionItem.template || sessionItem.id || ''; }).filter(Boolean),
                hooks: (results[2].items || []).map(function(bead) { return bead.id; }),
                messages: (results[3].items || []).map(function(message) { return message.id; }),
                convoys: []
            };
            options.clearError();
            return cachedOptions;
        }).catch(function(err) {
            options.handleError(err, 'fetchOptions');
            return null;
        });
    }

    function getOptionsForType(argType) {
        if (!cachedOptions) {
            return [];
        }
        var rawOptions;
        switch (argType) {
        case 'rigs':
            rawOptions = cachedOptions.rigs || [];
            break;
        case 'agents':
            rawOptions = cachedOptions.agents || [];
            break;
        case 'hooks':
            rawOptions = cachedOptions.hooks || [];
            break;
        case 'messages':
            rawOptions = cachedOptions.messages || [];
            break;
        case 'convoys':
            rawOptions = cachedOptions.convoys || [];
            break;
        default:
            return [];
        }
        return rawOptions.map(function(opt) {
            if (typeof opt === 'string') {
                return {value: opt, label: opt, disabled: false};
            }
            return {value: opt.name || '', label: opt.name || '', disabled: false};
        });
    }

    function dispatchCommandAsWSAction(cmdStr) {
        var parts = cmdStr.trim().split(/\s+/);
        var cmd = parts[0];
        var subcmd = parts[1] || '';
        var args = parts.slice(2);

        switch (cmd) {
        case 'status':
            return options.wsRequest('status.get');
        case 'sling':
            return options.wsRequest('sling.run', {bead_id: parts[1], rig: parts[2] || ''});
        case 'unsling':
            return options.wsRequest('bead.assign', {id: parts[1], assignee: ''});
        case 'mail':
            switch (subcmd) {
            case 'inbox':
            case 'check':
                return options.wsRequest('mail.list');
            case 'send':
                return options.wsRequest('mail.send', {to: args[0], subject: args.slice(1).join(' '), body: ''});
            case 'read':
            case 'mark-read':
                return options.wsRequest('mail.read', {id: args[0]});
            case 'archive':
                return options.wsRequest('mail.archive', {id: args[0]});
            case 'mark-unread':
                return options.wsRequest('mail.mark_unread', {id: args[0]});
            default:
                return Promise.reject(new Error('Unknown mail command: ' + subcmd));
            }
        case 'convoy':
            switch (subcmd) {
            case 'list':
                return options.wsRequest('convoys.list');
            case 'status':
            case 'show':
                return options.wsRequest('convoy.get', {id: args[0]});
            case 'create':
                return options.wsRequest('convoy.create', {title: args[0], items: args.slice(1)});
            case 'add':
                return options.wsRequest('convoy.add', {id: args[0], items: args.slice(1)});
            default:
                return Promise.reject(new Error('Unknown convoy command: ' + subcmd));
            }
        case 'rig':
            switch (subcmd) {
            case 'list':
                return options.wsRequest('rigs.list');
            default:
                return Promise.reject(new Error('Unknown rig command: ' + subcmd));
            }
        case 'agent':
            switch (subcmd) {
            case 'list':
                return options.wsRequest('agents.list');
            default:
                return Promise.reject(new Error('Unknown agent command: ' + subcmd));
            }
        case 'config':
            switch (subcmd) {
            case 'get':
                return options.wsRequest('config.get');
            case 'validate':
                return options.wsRequest('config.validate');
            default:
                return Promise.reject(new Error('Unknown config command: ' + subcmd));
            }
        default:
            return Promise.reject(new Error('Command not available over WebSocket: ' + cmd));
        }
    }

    function scoreCommand(cmd, query) {
        var name = cmd.name.toLowerCase();
        var desc = (cmd.desc || '').toLowerCase();
        var cat = (cmd.category || '').toLowerCase();
        var q = query.toLowerCase();

        if (name.indexOf(q) === 0) {
            return 100 + (50 - name.length);
        }
        var nameParts = name.split(' ');
        for (var i = 0; i < nameParts.length; i++) {
            if (nameParts[i].indexOf(q) === 0) {
                return 80 + (50 - name.length);
            }
        }
        if (name.indexOf(q) !== -1) {
            return 60 + (50 - name.length);
        }
        if (desc.indexOf(q) !== -1) {
            return 40;
        }
        if (cat.indexOf(q) !== -1) {
            return 20;
        }
        var nameIndex = 0;
        for (var queryIndex = 0; queryIndex < q.length; queryIndex++) {
            nameIndex = name.indexOf(q[queryIndex], nameIndex);
            if (nameIndex === -1) {
                return -1;
            }
            nameIndex++;
        }
        return 10;
    }

    function highlightMatch(text, query) {
        if (!query) {
            return options.escapeHtml(text);
        }
        var lowerText = text.toLowerCase();
        var lowerQuery = query.toLowerCase();
        var idx = lowerText.indexOf(lowerQuery);
        if (idx !== -1) {
            return options.escapeHtml(text.substring(0, idx)) +
                '<mark>' + options.escapeHtml(text.substring(idx, idx + query.length)) + '</mark>' +
                options.escapeHtml(text.substring(idx + query.length));
        }
        return options.escapeHtml(text);
    }

    function detectActiveContext() {
        var expandedPanel = document.querySelector('.panel.expanded');
        if (expandedPanel) {
            var panelId = expandedPanel.id || '';
            if (panelId.indexOf('mail') !== -1) {
                return 'Mail';
            }
            if (panelId.indexOf('crew') !== -1) {
                return 'System';
            }
            if (panelId.indexOf('bead') !== -1 || panelId.indexOf('issue') !== -1) {
                return 'Work';
            }
            if (panelId.indexOf('convoy') !== -1) {
                return 'Work';
            }
        }
        var mailDetail = document.getElementById('mail-detail');
        var mailCompose = document.getElementById('mail-compose');
        if ((mailDetail && mailDetail.style.display !== 'none') ||
            (mailCompose && mailCompose.style.display !== 'none')) {
            return 'Mail';
        }
        var issueDetail = document.getElementById('issue-detail');
        if (issueDetail && issueDetail.style.display !== 'none') {
            return 'Work';
        }
        return null;
    }

    function parseArgsTemplate(argsStr) {
        if (!argsStr) {
            return [];
        }
        var args = [];
        var regex = /(?:(-\w+)\s+)?<([^>]+)>/g;
        var match;
        while ((match = regex.exec(argsStr)) !== null) {
            args.push({name: match[2], flag: match[1] || null});
        }
        return args;
    }

    function showOutput(cmd, output) {
        if (outputCmd) {
            outputCmd.textContent = 'gc ' + cmd;
        }
        if (outputContent) {
            outputContent.textContent = typeof output === 'string' ? output : JSON.stringify(output, null, 2);
        }
        if (outputPanel) {
            outputPanel.classList.add('open');
        }
    }

    function filterCommands(query) {
        query = (query || '').trim();
        if (!query) {
            visibleCommands = [];
            var shownNames = {};

            var recentItems = [];
            for (var recentIndex = 0; recentIndex < recentCommands.length; recentIndex++) {
                var recentCmd = allCommands.find(function(c) { return c.name === recentCommands[recentIndex]; });
                if (recentCmd) {
                    recentItems.push(recentCmd);
                }
            }
            if (recentItems.length > 0) {
                visibleCommands.push({_section: 'Recent'});
                for (var ri = 0; ri < recentItems.length; ri++) {
                    var command = Object.assign({}, recentItems[ri], {_recent: true});
                    visibleCommands.push(command);
                    shownNames[command.name] = true;
                }
            }

            var context = detectActiveContext();
            if (context) {
                var contextItems = allCommands.filter(function(c) {
                    return c.category === context && !shownNames[c.name];
                });
                if (contextItems.length > 0) {
                    visibleCommands.push({_section: 'Suggested — ' + context});
                    for (var ci = 0; ci < contextItems.length; ci++) {
                        visibleCommands.push(contextItems[ci]);
                        shownNames[contextItems[ci].name] = true;
                    }
                }
            }

            var remaining = allCommands.filter(function(c) { return !shownNames[c.name]; });
            remaining.sort(function(a, b) { return a.name.localeCompare(b.name); });
            if (remaining.length > 0) {
                visibleCommands.push({_section: 'All Commands'});
                for (var ai = 0; ai < remaining.length; ai++) {
                    visibleCommands.push(remaining[ai]);
                }
            }
        } else {
            var scored = [];
            for (var i = 0; i < allCommands.length; i++) {
                var score = scoreCommand(allCommands[i], query);
                if (score > 0) {
                    scored.push({cmd: allCommands[i], score: score});
                }
            }
            scored.sort(function(a, b) { return b.score - a.score; });
            visibleCommands = scored.map(function(item) { return item.cmd; });
        }
        selectedIdx = 0;
        while (selectedIdx < visibleCommands.length && visibleCommands[selectedIdx]._section) {
            selectedIdx++;
        }
        renderPaletteResults();
    }

    function renderPaletteResults() {
        if (!resultsDiv) {
            return;
        }

        if (pendingCommand) {
            var optionsForCommand = pendingCommand.argType ? getOptionsForType(pendingCommand.argType) : [];
            var argFields = parseArgsTemplate(pendingCommand.args);

            var formHtml = '<div class="command-args-prompt">' +
                '<div class="command-args-header">gc ' + options.escapeHtml(pendingCommand.name) + '</div>';

            for (var i = 0; i < argFields.length; i++) {
                var field = argFields[i];
                var fieldId = 'arg-field-' + i;
                var isFirstField = i === 0 && !field.flag;
                var hasOptions = isFirstField && pendingCommand.argType && optionsForCommand.length > 0;
                var noOptions = isFirstField && pendingCommand.argType && optionsForCommand.length === 0;
                var isMessageField = field.name === 'message' || field.name === 'body';

                formHtml += '<div class="command-field">';
                formHtml += '<label class="command-field-label" for="' + fieldId + '">' + options.escapeHtml(field.name) + '</label>';

                if (hasOptions) {
                    formHtml += '<select id="' + fieldId + '" class="command-field-select" data-flag="' + (field.flag || '') + '">';
                    formHtml += '<option value="">Select ' + options.escapeHtml(field.name) + '...</option>';
                    for (var j = 0; j < optionsForCommand.length; j++) {
                        var opt = optionsForCommand[j];
                        var disabledAttr = opt.disabled ? ' disabled' : '';
                        formHtml += '<option value="' + options.escapeHtml(opt.value) + '"' + disabledAttr + '>' + options.escapeHtml(opt.label) + '</option>';
                    }
                    formHtml += '</select>';
                } else if (noOptions) {
                    formHtml += '<input type="text" id="' + fieldId + '" class="command-field-input" data-flag="' + (field.flag || '') + '" placeholder="No ' + options.escapeHtml(pendingCommand.argType) + ' available">';
                } else if (isMessageField) {
                    formHtml += '<textarea id="' + fieldId + '" class="command-field-textarea" data-flag="' + (field.flag || '') + '" placeholder="Enter ' + options.escapeHtml(field.name) + '..." rows="3"></textarea>';
                } else {
                    formHtml += '<input type="text" id="' + fieldId + '" class="command-field-input" data-flag="' + (field.flag || '') + '" placeholder="Enter ' + options.escapeHtml(field.name) + '...">';
                }
                formHtml += '</div>';
            }

            if (argFields.length === 0 && pendingCommand.args) {
                formHtml += '<div class="command-field">';
                formHtml += '<input type="text" id="arg-field-0" class="command-field-input" placeholder="' + options.escapeHtml(pendingCommand.args) + '">';
                formHtml += '</div>';
            }

            formHtml += '<div class="command-args-actions">' +
                '<button id="command-args-run" class="command-args-btn run">Run</button>' +
                '<button id="command-args-cancel" class="command-args-btn cancel">Cancel</button>' +
                '</div></div>';

            resultsDiv.innerHTML = formHtml;

            var firstField = resultsDiv.querySelector('#arg-field-0');
            if (firstField) {
                firstField.focus();
            }

            var runBtn = document.getElementById('command-args-run');
            var cancelBtn = document.getElementById('command-args-cancel');

            if (runBtn) {
                runBtn.onclick = function() {
                    runBtn.classList.add('loading');
                    runBtn.textContent = 'Running';
                    runWithArgsFromForm(argFields.length || 1);
                };
            }
            if (cancelBtn) {
                cancelBtn.onclick = function() {
                    pendingCommand = null;
                    filterCommands(searchInput ? searchInput.value : '');
                };
            }

            resultsDiv.querySelectorAll('input, select, textarea').forEach(function(el) {
                el.onkeydown = function(ev) {
                    if (ev.key === 'Enter' && el.tagName !== 'TEXTAREA') {
                        ev.preventDefault();
                        runWithArgsFromForm(argFields.length || 1);
                    } else if (ev.key === 'Escape') {
                        ev.preventDefault();
                        pendingCommand = null;
                        filterCommands(searchInput ? searchInput.value : '');
                    }
                };
            });
            return;
        }

        if (visibleCommands.length === 0) {
            resultsDiv.innerHTML = '<div class="command-palette-empty">No matching commands</div>';
            return;
        }

        var currentQuery = searchInput ? searchInput.value.trim() : '';
        var html = '';

        if (currentQuery) {
            for (var i2 = 0; i2 < visibleCommands.length; i2++) {
                var cmd = visibleCommands[i2];
                var cls = 'command-item' + (i2 === selectedIdx ? ' selected' : '');
                var argsHint = cmd.args ? ' <span class="command-args">' + options.escapeHtml(cmd.args) + '</span>' : '';
                var nameHtml = highlightMatch('gc ' + cmd.name, currentQuery);
                html += '<div class="' + cls + '" data-cmd-name="' + options.escapeHtml(cmd.name) + '" data-cmd-args="' + options.escapeHtml(cmd.args || '') + '">' +
                    '<span class="command-name">' + nameHtml + argsHint + '</span>' +
                    '<span class="command-desc">' + options.escapeHtml(cmd.desc || '') + '</span>' +
                    '<span class="command-category">' + options.escapeHtml(cmd.category || '') + '</span>' +
                    '</div>';
            }
        } else {
            for (var j2 = 0; j2 < visibleCommands.length; j2++) {
                var item = visibleCommands[j2];
                if (item._section) {
                    html += '<div class="command-section-header">' + options.escapeHtml(item._section) + '</div>';
                    continue;
                }
                var cls2 = 'command-item' + (j2 === selectedIdx ? ' selected' : '');
                var argsHint2 = item.args ? ' <span class="command-args">' + options.escapeHtml(item.args) + '</span>' : '';
                var icon2 = item._recent ? '<span class="command-recent-icon">↻</span>' : '';
                html += '<div class="' + cls2 + '" data-cmd-name="' + options.escapeHtml(item.name) + '" data-cmd-args="' + options.escapeHtml(item.args || '') + '">' +
                    icon2 +
                    '<span class="command-name">gc ' + options.escapeHtml(item.name) + argsHint2 + '</span>' +
                    '<span class="command-desc">' + options.escapeHtml(item.desc || '') + '</span>' +
                    '<span class="command-category">' + options.escapeHtml(item.category || '') + '</span>' +
                    '</div>';
            }
        }
        resultsDiv.innerHTML = html;

        var selectedEl = resultsDiv.querySelector('.command-item.selected');
        if (selectedEl) {
            selectedEl.scrollIntoView({block: 'nearest'});
        }
    }

    function runWithArgsFromForm(fieldCount) {
        var args = [];
        for (var i = 0; i < fieldCount; i++) {
            var field = document.getElementById('arg-field-' + i);
            if (!field) {
                continue;
            }
            var val = field.value.trim();
            var flag = field.getAttribute('data-flag');
            if (val) {
                if (flag) {
                    args.push(flag);
                    args.push('"' + val.replace(/"/g, '\\"') + '"');
                } else {
                    args.push(val);
                }
            }
        }
        if (pendingCommand) {
            var fullCmd = pendingCommand.name + (args.length ? ' ' + args.join(' ') : '');
            pendingCommand = null;
            runCommand(fullCmd);
        }
    }

    function openPalette() {
        isPaletteOpen = true;
        pendingCommand = null;
        if (overlay) {
            overlay.style.display = 'flex';
            overlay.classList.add('open');
        }
        if (searchInput) {
            searchInput.value = '';
            searchInput.focus();
        }
        filterCommands('');
        fetchOptions();
    }

    function closePalette() {
        isPaletteOpen = false;
        pendingCommand = null;
        if (overlay) {
            overlay.classList.remove('open');
            overlay.style.display = 'none';
        }
        if (searchInput) {
            searchInput.value = '';
        }
        visibleCommands = [];
        if (resultsDiv) {
            resultsDiv.innerHTML = '';
        }
    }

    function selectCommand(cmdName, cmdArgs) {
        if (cmdArgs) {
            var cmd = allCommands.find(function(c) { return c.name === cmdName; });
            if (cmd) {
                pendingCommand = cmd;
                if (cmd.argType && !cachedOptions) {
                    fetchOptions().then(function() { renderPaletteResults(); });
                } else {
                    renderPaletteResults();
                }
                return;
            }
        }
        runCommand(cmdName);
    }

    function runCommand(cmdName) {
        if (executionLock || !cmdName) {
            return;
        }

        closePalette();

        var baseName = cmdName.split(' ').slice(0, 3).join(' ');
        var matchedCmd = allCommands.find(function(c) { return cmdName.indexOf(c.name) === 0; });
        saveRecentCommand(matchedCmd ? matchedCmd.name : baseName);

        executionLock = true;
        options.showToast('info', 'Running...', 'gc ' + cmdName);

        dispatchCommandAsWSAction(cmdName).then(function(data) {
            options.showToast('success', 'Success', 'gc ' + cmdName);
            if (data && typeof data === 'object') {
                showOutput(cmdName, JSON.stringify(data, null, 2));
            } else if (data && typeof data === 'string' && data.trim()) {
                showOutput(cmdName, data);
            }
        }).catch(function(err) {
            options.handleError(err, 'palette.command');
            options.showToast('error', 'Error', err.message || 'Request failed');
        }).finally(function() {
            setTimeout(function() { executionLock = false; }, 1000);
        });
    }

    function init() {
        loadRecentCommands();

        options.on('output-close-btn', 'click', function() {
            if (outputPanel) {
                outputPanel.classList.remove('open');
            }
        });

        options.on('output-copy-btn', 'click', function() {
            if (!outputContent) {
                return;
            }
            navigator.clipboard.writeText(outputContent.textContent).then(function() {
                options.showToast('success', 'Copied', 'Output copied to clipboard');
            }).catch(function(err) {
                options.handleError(err, 'palette.copy');
            });
        });

        if (resultsDiv) {
            resultsDiv.addEventListener('click', function(e) {
                var item = e.target.closest('.command-item');
                if (!item) {
                    return;
                }
                e.preventDefault();
                e.stopPropagation();
                var cmdName = item.getAttribute('data-cmd-name');
                var cmdArgs = item.getAttribute('data-cmd-args');
                if (cmdName) {
                    selectCommand(cmdName, cmdArgs);
                }
            });
        }

        document.addEventListener('click', function(e) {
            if (e.target.closest('#open-palette-btn')) {
                e.preventDefault();
                openPalette();
                return;
            }
            if (e.target === overlay) {
                closePalette();
            }
        });

        document.addEventListener('keydown', function(e) {
            if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
                e.preventDefault();
                if (isPaletteOpen) {
                    closePalette();
                } else {
                    openPalette();
                }
                return;
            }

            if (!isPaletteOpen && e.key === 'Escape') {
                var expanded = document.querySelector('.panel.expanded');
                if (expanded) {
                    e.preventDefault();
                    expanded.classList.remove('expanded');
                    var expandBtn = expanded.querySelector('.expand-btn');
                    if (expandBtn) {
                        expandBtn.textContent = 'Expand';
                    }
                    window.pauseRefresh = false;
                    return;
                }
            }

            if (!isPaletteOpen || pendingCommand) {
                return;
            }

            if (e.key === 'Escape') {
                e.preventDefault();
                closePalette();
                return;
            }

            if (e.key === 'ArrowDown') {
                e.preventDefault();
                if (visibleCommands.length > 0) {
                    var next = selectedIdx + 1;
                    while (next < visibleCommands.length && visibleCommands[next]._section) {
                        next++;
                    }
                    if (next < visibleCommands.length) {
                        selectedIdx = next;
                    }
                    renderPaletteResults();
                }
                return;
            }

            if (e.key === 'ArrowUp') {
                e.preventDefault();
                var prev = selectedIdx - 1;
                while (prev >= 0 && visibleCommands[prev]._section) {
                    prev--;
                }
                if (prev >= 0) {
                    selectedIdx = prev;
                }
                renderPaletteResults();
                return;
            }

            if (e.key === 'Enter') {
                e.preventDefault();
                var selected = visibleCommands[selectedIdx];
                if (selected && !selected._section) {
                    selectCommand(selected.name, selected.args);
                }
            }
        });

        if (searchInput) {
            searchInput.addEventListener('input', function() {
                filterCommands(searchInput.value);
            });
        }
    }

    return {
        buildCommandsFromCapabilities: buildCommandsFromCapabilities,
        init: init
    };
}
