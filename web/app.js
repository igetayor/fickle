/**
 * Fickle WebSocket Interactive Console & Real-Time Dashboard
 */
(function () {
  'use strict';

  // State Management
  let ws = null;
  let currentRoom = 'crypto-ticks';
  let activeRooms = new Set(['crypto-ticks']);
  let frameCount = 0;
  let burstInterval = null;
  let burstSent = 0;
  let burstRecv = 0;
  let burstStartTime = null;
  let lastPingTimestamp = 0;

  // DOM Elements
  const btnToggleConnect = document.getElementById('btnToggleConnect');
  const connStatus = document.getElementById('connStatus');
  const connDot = document.getElementById('connDot');
  const nodeIdEl = document.getElementById('nodeId');
  const redisStatus = document.getElementById('redisStatus');
  const redisDot = document.getElementById('redisDot');

  const metricConnections = document.getElementById('metricConnections');
  const metricTotalSessions = document.getElementById('metricTotalSessions');
  const metricRooms = document.getElementById('metricRooms');
  const metricRoomsList = document.getElementById('metricRoomsList');
  const metricMsgIn = document.getElementById('metricMsgIn');
  const metricMsgOut = document.getElementById('metricMsgOut');
  const metricLatency = document.getElementById('metricLatency');
  const metricUptime = document.getElementById('metricUptime');

  const streamFeed = document.getElementById('streamFeed');
  const streamCounter = document.getElementById('streamCounter');
  const chkAutoScroll = document.getElementById('chkAutoScroll');
  const btnClearStream = document.getElementById('btnClearStream');

  const inputNickname = document.getElementById('inputNickname');
  const btnUpdateNick = document.getElementById('btnUpdateNick');
  const roomChips = document.getElementById('roomChips');
  const inputCustomRoom = document.getElementById('inputCustomRoom');
  const btnJoinRoom = document.getElementById('btnJoinRoom');
  const btnLeaveRoom = document.getElementById('btnLeaveRoom');

  const selectAction = document.getElementById('selectAction');
  const groupTargetId = document.getElementById('groupTargetId');
  const inputTargetId = document.getElementById('inputTargetId');
  const jsonPayload = document.getElementById('jsonPayload');
  const btnSendFrame = document.getElementById('btnSendFrame');
  const btnQuickPing = document.getElementById('btnQuickPing');

  const tabButtons = document.querySelectorAll('.tab-btn');
  const tabContents = document.querySelectorAll('.tab-content');

  const rateSlider = document.getElementById('rateSlider');
  const rateLabel = document.getElementById('rateLabel');
  const selectBurstRoom = document.getElementById('selectBurstRoom');
  const btnStartBurst = document.getElementById('btnStartBurst');
  const btnStopBurst = document.getElementById('btnStopBurst');
  const burstSentCount = document.getElementById('burstSentCount');
  const burstRecvCount = document.getElementById('burstRecvCount');
  const burstRateDisplay = document.getElementById('burstRateDisplay');

  // Initialize Event Listeners
  initTabs();
  initWebSocketEvents();
  initBurstEvents();
  startMetricsPolling();

  function initTabs() {
    tabButtons.forEach(btn => {
      btn.addEventListener('click', () => {
        tabButtons.forEach(b => b.classList.remove('active'));
        tabContents.forEach(c => c.classList.remove('active'));
        btn.classList.add('active');
        const targetId = btn.getAttribute('data-tab');
        document.getElementById(targetId).classList.add('active');
      });
    });
  }

  function initWebSocketEvents() {
    btnToggleConnect.addEventListener('click', () => {
      if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) {
        disconnectWS();
      } else {
        connectWS();
      }
    });

    btnUpdateNick.addEventListener('click', () => {
      const nick = inputNickname.value.trim();
      if (!nick) return;
      const sent = sendFrame({
        action: 'broadcast',
        room: currentRoom,
        nickname: nick,
        data: { message: `Nickname updated to ${nick}` }
      });
      if (sent) {
        const origText = btnUpdateNick.textContent;
        btnUpdateNick.textContent = 'Saved ✓';
        setTimeout(() => {
          btnUpdateNick.textContent = origText;
        }, 800);
      }
    });

    // Room chip clicks
    roomChips.addEventListener('click', (e) => {
      const chip = e.target.closest('.chip');
      if (!chip) return;
      const room = chip.getAttribute('data-room');
      selectRoom(room);
    });

    btnJoinRoom.addEventListener('click', () => {
      const room = inputCustomRoom.value.trim();
      if (!room) return;
      joinRoom(room);
      inputCustomRoom.value = '';
    });

    btnLeaveRoom.addEventListener('click', () => {
      const room = inputCustomRoom.value.trim() || currentRoom;
      leaveRoom(room);
    });

    selectAction.addEventListener('change', () => {
      groupTargetId.style.display = selectAction.value === 'direct_msg' ? 'block' : 'none';
    });

    btnSendFrame.addEventListener('click', () => {
      const action = selectAction.value;
      const rawText = jsonPayload.value.trim();
      let data = {};
      if (rawText) {
        if (rawText.startsWith('{') || rawText.startsWith('[')) {
          try {
            data = JSON.parse(rawText);
          } catch (err) {
            alert('Invalid JSON in payload data area: ' + err.message);
            return;
          }
        } else {
          data = { message: rawText };
        }
      }

      const frame = {
        action: action,
        room: currentRoom,
        data: data
      };

      if (action === 'direct_msg') {
        frame.target_id = inputTargetId.value.trim();
        if (!frame.target_id) {
          alert('Please enter a target Client ID or Session ID');
          return;
        }
      }

      const sent = sendFrame(frame);
      if (sent) {
        const origHTML = btnSendFrame.innerHTML;
        btnSendFrame.textContent = 'Sent ✓';
        btnSendFrame.style.background = '#10b981';
        setTimeout(() => {
          btnSendFrame.innerHTML = origHTML;
          btnSendFrame.style.background = '';
        }, 800);
      }
    });

    btnQuickPing.addEventListener('click', () => {
      lastPingTimestamp = performance.now();
      const sent = sendFrame({ action: 'ping' });
      if (sent) {
        const origText = btnQuickPing.textContent;
        btnQuickPing.textContent = 'Pinging...';
        setTimeout(() => {
          btnQuickPing.textContent = origText;
        }, 800);
      }
    });

    btnClearStream.addEventListener('click', () => {
      streamFeed.innerHTML = '';
      frameCount = 0;
      streamCounter.textContent = '0 frames';
    });

    // Auto-connect on load
    connectWS();
  }

  function getWsUrl() {
    const loc = window.location;
    const protocol = loc.protocol === 'https:' ? 'wss:' : 'ws:';
    const host = loc.host || 'localhost:8080';
    const nick = encodeURIComponent(inputNickname.value.trim());
    const room = encodeURIComponent(currentRoom);
    return `${protocol}//${host}/ws?nickname=${nick}&room=${room}`;
  }

  function connectWS() {
    const url = getWsUrl();
    setConnectionState('connecting');

    try {
      ws = new WebSocket(url);
    } catch (e) {
      setConnectionState('disconnected');
      renderFrame({
        type: 'error',
        data: { error: 'Failed to construct WebSocket: ' + e.message },
        timestamp: Date.now()
      });
      return;
    }

    ws.onopen = () => {
      setConnectionState('connected');
      // Re-join active rooms
      activeRooms.forEach(room => {
        sendFrame({ action: 'join_room', room: room });
      });
    };

    ws.onclose = (event) => {
      setConnectionState('disconnected');
      renderFrame({
        type: 'system',
        data: { message: `WebSocket closed (code: ${event.code}, reason: ${event.reason || 'none'})` },
        timestamp: Date.now()
      });
    };

    ws.onerror = (err) => {
      console.error('WebSocket Error:', err);
      setConnectionState('disconnected');
    };

    ws.onmessage = (event) => {
      // In case writePump coalesced multiple newline-separated frames
      const lines = event.data.split('\n');
      lines.forEach(line => {
        if (!line.trim()) return;
        try {
          const parsed = JSON.parse(line);
          handleIncomingFrame(parsed);
        } catch (e) {
          console.warn('Non-JSON frame received:', line);
        }
      });
    };
  }

  function disconnectWS() {
    if (ws) {
      ws.close();
      ws = null;
    }
    setConnectionState('disconnected');
  }

  function setConnectionState(state) {
    if (state === 'connected') {
      connStatus.textContent = 'Connected';
      connDot.className = 'dot green';
      btnToggleConnect.textContent = 'Disconnect';
      btnToggleConnect.className = 'btn btn-danger btn-sm';
    } else if (state === 'connecting') {
      connStatus.textContent = 'Connecting...';
      connDot.className = 'dot yellow';
      btnToggleConnect.textContent = 'Connecting';
      btnToggleConnect.className = 'btn btn-secondary btn-sm';
    } else {
      connStatus.textContent = 'Disconnected';
      connDot.className = 'dot red';
      btnToggleConnect.textContent = 'Connect';
      btnToggleConnect.className = 'btn btn-primary btn-sm';
    }
  }

  function sendFrame(frame) {
    if (!ws || ws.readyState !== WebSocket.OPEN) {

      connectWS();
      // Try again after a short delay
      setTimeout(() => {
        if (ws && ws.readyState === WebSocket.OPEN) {
          frame.trace_id = crypto.randomUUID ? crypto.randomUUID() : 'trc-' + Math.random().toString(36).substring(2, 9);
          ws.send(JSON.stringify(frame));
        } else {
          alert('Connecting to WebSocket at ' + getWsUrl() + '... Please ensure backend is running at http://localhost:8080 and click "Connect".');
        }
      }, 300);
      return false;
    }
    frame.trace_id = crypto.randomUUID ? crypto.randomUUID() : 'trc-' + Math.random().toString(36).substring(2, 9);
    ws.send(JSON.stringify(frame));
    return true;
  }

  function handleIncomingFrame(frame) {
    frameCount++;
    streamCounter.textContent = `${frameCount} frames`;

    // Measure ping latency if pong
    if (frame.action === 'pong' && lastPingTimestamp > 0) {
      const rtt = Math.round(performance.now() - lastPingTimestamp);
      metricLatency.textContent = `${rtt} ms`;
      lastPingTimestamp = 0;
    }

    if (burstInterval) {
      burstRecv++;
      burstRecvCount.textContent = burstRecv;
    }

    renderFrame(frame);
  }

  function renderFrame(frame) {
    const placeholder = streamFeed.querySelector('.stream-placeholder');
    if (placeholder) {
      placeholder.remove();
    }

    const item = document.createElement('div');
    const frameType = frame.type || 'message';
    item.className = `frame-item ${frameType}`;

    const timeStr = frame.timestamp ? new Date(frame.timestamp).toLocaleTimeString() : new Date().toLocaleTimeString();
    const roomStr = frame.room ? `<span class="badge badge-room">#${frame.room}</span>` : '';
    const senderStr = frame.sender ? `<span class="badge badge-sender">${frame.sender.nickname || frame.sender.id.substring(0, 8)}</span>` : '';
    const nodeStr = frame.node_id ? `<span class="badge" style="background:rgba(255,255,255,0.08);color:#94a3b8;">${frame.node_id}</span>` : '';

    item.innerHTML = `
      <div class="frame-header">
        <div class="frame-meta">
          <span class="badge">${frameType}</span>
          ${roomStr}
          ${senderStr}
          ${nodeStr}
        </div>
        <span class="frame-time">${timeStr}</span>
      </div>
      <div class="frame-body">${escapeHtml(JSON.stringify(frame.data || frame, null, 2))}</div>
    `;

    streamFeed.appendChild(item);

    // Limit DOM items to 200 to keep UI ultra responsive
    if (streamFeed.children.length > 200) {
      streamFeed.removeChild(streamFeed.firstElementChild);
    }

    if (chkAutoScroll.checked) {
      streamFeed.scrollTop = streamFeed.scrollHeight;
    }
  }

  function escapeHtml(str) {
    return str
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');
  }

  function selectRoom(room) {
    currentRoom = room;
    // Update chips UI
    document.querySelectorAll('.chip').forEach(c => {
      c.classList.toggle('active', c.getAttribute('data-room') === room);
    });

    joinRoom(room);
  }

  function joinRoom(room) {
    activeRooms.add(room);
    sendFrame({ action: 'join_room', room: room });
  }

  function leaveRoom(room) {
    activeRooms.delete(room);
    sendFrame({ action: 'leave_room', room: room });
  }

  // Burst & Stress Generator
  function initBurstEvents() {
    rateSlider.addEventListener('input', () => {
      rateLabel.textContent = `${rateSlider.value} messages / sec`;
    });

    btnStartBurst.addEventListener('click', () => {
      const rate = parseInt(rateSlider.value, 10);
      const targetRoom = selectBurstRoom.value;
      burstSent = 0;
      burstRecv = 0;
      burstSentCount.textContent = '0';
      burstRecvCount.textContent = '0';
      burstStartTime = performance.now();

      btnStartBurst.disabled = true;
      btnStopBurst.disabled = false;

      const intervalMs = Math.floor(1000 / rate);
      burstInterval = setInterval(() => {
        if (!ws || ws.readyState !== WebSocket.OPEN) {
          stopBurst();
          return;
        }

        burstSent++;
        burstSentCount.textContent = burstSent;
        sendFrame({
          action: 'broadcast',
          room: targetRoom,
          data: {
            burst_seq: burstSent,
            source: 'stress_generator',
            metric_val: (Math.random() * 100).toFixed(2),
            timestamp: Date.now()
          }
        });

        // Update rate display
        const elapsedSec = (performance.now() - burstStartTime) / 1000;
        if (elapsedSec > 0.5) {
          const actualRate = (burstSent / elapsedSec).toFixed(1);
          burstRateDisplay.textContent = `${actualRate} msg/s`;
        }
      }, intervalMs);
    });

    btnStopBurst.addEventListener('click', stopBurst);
  }

  function stopBurst() {
    if (burstInterval) {
      clearInterval(burstInterval);
      burstInterval = null;
    }
    btnStartBurst.disabled = false;
    btnStopBurst.disabled = true;
  }

  // Periodic Telemetry & Metrics Polling
  function startMetricsPolling() {
    const fetchMetrics = async () => {
      try {
        const res = await fetch('/api/v1/metrics');
        if (!res.ok) return;
        const data = await res.json();

        nodeIdEl.textContent = data.node_id || 'local';
        metricConnections.textContent = data.active_connections;
        metricTotalSessions.textContent = `${data.total_sessions} total sessions`;
        metricRooms.textContent = data.active_rooms_count;
        metricMsgIn.textContent = data.total_messages_in;
        metricMsgOut.textContent = data.total_messages_out;
        metricUptime.textContent = `Uptime: ${formatUptime(data.uptime_seconds)}`;

        if (data.rooms && data.rooms.length > 0) {
          const roomSummary = data.rooms.map(r => `${r.name} (${r.client_count})`).join(', ');
          metricRoomsList.textContent = roomSummary;
        } else {
          metricRoomsList.textContent = 'No active rooms';
        }

        // Redis status
        if (data.redis_connected) {
          redisStatus.textContent = 'Connected';
          redisDot.className = 'dot green';
        } else {
          redisStatus.textContent = 'Standalone';
          redisDot.className = 'dot yellow';
        }
      } catch (err) {
        // Server might be starting
      }
    };

    fetchMetrics();
    setInterval(fetchMetrics, 2000);
  }

  function formatUptime(seconds) {
    if (!seconds) return '0s';
    const m = Math.floor(seconds / 60);
    const s = seconds % 60;
    if (m === 0) return `${s}s`;
    const h = Math.floor(m / 60);
    return `${h}h ${m % 60}m ${s}s`;
  }
})();
