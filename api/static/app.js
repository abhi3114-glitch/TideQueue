/**
 * TideQueue Admin Dashboard JavaScript
 */

// API Base URL
const API_BASE = '/api';

// Refresh interval (5 seconds)
const REFRESH_INTERVAL = 5000;

// State
let currentSection = 'dashboard';
let refreshTimer = null;

// DOM Elements
const navItems = document.querySelectorAll('.nav-item');
const sections = document.querySelectorAll('.section');
const pageTitle = document.getElementById('page-title');
const newJobBtn = document.getElementById('new-job-btn');
const refreshBtn = document.getElementById('refresh-btn');
const newJobModal = document.getElementById('new-job-modal');
const closeModalBtn = document.getElementById('close-modal');
const cancelJobBtn = document.getElementById('cancel-job');
const newJobForm = document.getElementById('new-job-form');
const jobStatusFilter = document.getElementById('job-status-filter');
const scaleBtn = document.getElementById('scale-btn');
const purgeDlqBtn = document.getElementById('purge-dlq-btn');

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    initNavigation();
    initModal();
    initFilters();
    initActions();
    loadData();
    startAutoRefresh();
});

// Navigation
function initNavigation() {
    navItems.forEach(item => {
        item.addEventListener('click', () => {
            const section = item.dataset.section;
            switchSection(section);
        });
    });
}

function switchSection(sectionName) {
    // Update nav
    navItems.forEach(item => {
        item.classList.toggle('active', item.dataset.section === sectionName);
    });

    // Update sections
    sections.forEach(section => {
        section.classList.toggle('active', section.id === `${sectionName}-section`);
    });

    // Update title
    const titles = {
        dashboard: 'Dashboard',
        jobs: 'Jobs',
        workers: 'Workers',
        queues: 'Queues',
        dlq: 'Dead-Letter Queue'
    };
    pageTitle.textContent = titles[sectionName] || 'Dashboard';

    currentSection = sectionName;
    loadData();
}

// Modal
function initModal() {
    newJobBtn.addEventListener('click', () => {
        newJobModal.classList.add('active');
    });

    closeModalBtn.addEventListener('click', () => {
        newJobModal.classList.remove('active');
    });

    cancelJobBtn.addEventListener('click', () => {
        newJobModal.classList.remove('active');
    });

    newJobModal.addEventListener('click', (e) => {
        if (e.target === newJobModal) {
            newJobModal.classList.remove('active');
        }
    });

    newJobForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        await submitJob();
    });
}

// Filters
function initFilters() {
    jobStatusFilter.addEventListener('change', loadJobs);
}

// Actions
function initActions() {
    refreshBtn.addEventListener('click', loadData);

    scaleBtn.addEventListener('click', async () => {
        const count = document.getElementById('worker-count').value;
        await scaleWorkers(parseInt(count));
    });

    purgeDlqBtn.addEventListener('click', async () => {
        if (confirm('Are you sure you want to purge all dead-letter jobs?')) {
            await purgeDlq();
        }
    });
}

// Auto Refresh
function startAutoRefresh() {
    if (refreshTimer) {
        clearInterval(refreshTimer);
    }
    refreshTimer = setInterval(loadData, REFRESH_INTERVAL);
}

// API Calls
async function fetchAPI(endpoint, options = {}) {
    try {
        const response = await fetch(`${API_BASE}${endpoint}`, {
            headers: {
                'Content-Type': 'application/json',
            },
            ...options,
        });

        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.detail || 'API Error');
        }

        return await response.json();
    } catch (error) {
        console.error('API Error:', error);
        throw error;
    }
}

// Load Data
async function loadData() {
    try {
        await loadStats();

        switch (currentSection) {
            case 'dashboard':
                await loadRecentJobs();
                break;
            case 'jobs':
                await loadJobs();
                break;
            case 'workers':
                await loadWorkers();
                break;
            case 'queues':
                await loadQueues();
                break;
            case 'dlq':
                await loadDlq();
                break;
        }
    } catch (error) {
        console.error('Failed to load data:', error);
    }
}

async function loadStats() {
    const stats = await fetchAPI('/stats');

    document.getElementById('stat-pending').textContent = stats.pending;
    document.getElementById('stat-running').textContent = stats.running;
    document.getElementById('stat-completed').textContent = stats.completed;
    document.getElementById('stat-failed').textContent = stats.failed;
    document.getElementById('stat-dead').textContent = stats.dead;
    document.getElementById('stat-workers').textContent = stats.workers.total_workers || 0;
}

async function loadRecentJobs() {
    const data = await fetchAPI('/jobs?limit=10');
    const tbody = document.getElementById('recent-jobs-body');

    if (data.jobs.length === 0) {
        tbody.innerHTML = '<tr><td colspan="6" class="empty-state">No jobs yet</td></tr>';
        return;
    }

    tbody.innerHTML = data.jobs.map(job => `
        <tr>
            <td><code>${job.id.substring(0, 8)}...</code></td>
            <td>${job.name}</td>
            <td>${job.queue}</td>
            <td><span class="status-badge ${job.status}">${job.status}</span></td>
            <td>${formatDate(job.created_at)}</td>
            <td>
                ${getJobActions(job)}
            </td>
        </tr>
    `).join('');
}

async function loadJobs() {
    const status = jobStatusFilter.value;
    const query = status ? `?status=${status}` : '';
    const data = await fetchAPI(`/jobs${query}`);
    const tbody = document.getElementById('jobs-body');

    if (data.jobs.length === 0) {
        tbody.innerHTML = '<tr><td colspan="8" class="empty-state">No jobs found</td></tr>';
        return;
    }

    tbody.innerHTML = data.jobs.map(job => `
        <tr>
            <td><code>${job.id.substring(0, 8)}...</code></td>
            <td>${job.name}</td>
            <td>${job.queue}</td>
            <td>${job.priority}</td>
            <td><span class="status-badge ${job.status}">${job.status}</span></td>
            <td>${job.retry_count}</td>
            <td>${formatDate(job.created_at)}</td>
            <td>
                ${getJobActions(job)}
            </td>
        </tr>
    `).join('');
}

async function loadWorkers() {
    const workers = await fetchAPI('/workers');
    const grid = document.getElementById('workers-grid');

    if (workers.length === 0) {
        grid.innerHTML = '<div class="empty-state">No workers running</div>';
        return;
    }

    grid.innerHTML = workers.map(worker => `
        <div class="worker-card">
            <div class="worker-header">
                <span class="worker-id">${worker.id}</span>
                <span class="status-badge ${worker.status === 'busy' ? 'running' : 'completed'}">
                    ${worker.status}
                </span>
            </div>
            <div class="worker-stats">
                <div class="worker-stat">
                    <div class="worker-stat-value">${worker.jobs_completed}</div>
                    <div class="worker-stat-label">Completed</div>
                </div>
                <div class="worker-stat">
                    <div class="worker-stat-value">${worker.jobs_failed}</div>
                    <div class="worker-stat-label">Failed</div>
                </div>
            </div>
        </div>
    `).join('');

    // Update scale input
    document.getElementById('worker-count').value = workers.length;
}

async function loadQueues() {
    const data = await fetchAPI('/queues');
    const tbody = document.getElementById('queues-body');

    if (data.queues.length === 0) {
        tbody.innerHTML = '<tr><td colspan="7" class="empty-state">No queues</td></tr>';
        return;
    }

    tbody.innerHTML = data.queues.map(queue => `
        <tr>
            <td><strong>${queue.name}</strong></td>
            <td>${queue.pending}</td>
            <td>${queue.running}</td>
            <td>${queue.completed}</td>
            <td>${queue.failed}</td>
            <td>${queue.dead}</td>
            <td><strong>${queue.total}</strong></td>
        </tr>
    `).join('');
}

async function loadDlq() {
    const data = await fetchAPI('/dlq');
    const tbody = document.getElementById('dlq-body');

    if (data.jobs.length === 0) {
        tbody.innerHTML = '<tr><td colspan="7" class="empty-state">No dead-letter jobs</td></tr>';
        return;
    }

    tbody.innerHTML = data.jobs.map(job => `
        <tr>
            <td><code>${job.id.substring(0, 8)}...</code></td>
            <td>${job.task}</td>
            <td>${job.queue}</td>
            <td>${job.retry_count}</td>
            <td title="${job.error || ''}">${(job.error || '').substring(0, 30)}...</td>
            <td>${formatDate(job.moved_at)}</td>
            <td>
                <button class="btn btn-small btn-primary" onclick="replayDlqJob('${job.id}')">
                    Replay
                </button>
                <button class="btn btn-small btn-danger" onclick="purgeDlqJob('${job.id}')">
                    Purge
                </button>
            </td>
        </tr>
    `).join('');
}

// Job Actions
function getJobActions(job) {
    const actions = [];

    if (job.status === 'pending' || job.status === 'scheduled') {
        actions.push(`<button class="btn btn-small btn-danger" onclick="cancelJob('${job.id}')">Cancel</button>`);
    }

    if (job.status === 'failed' || job.status === 'dead' || job.status === 'cancelled') {
        actions.push(`<button class="btn btn-small btn-primary" onclick="retryJob('${job.id}')">Retry</button>`);
    }

    return actions.join(' ') || '-';
}

async function cancelJob(jobId) {
    try {
        await fetchAPI(`/jobs/${jobId}`, { method: 'DELETE' });
        loadData();
    } catch (error) {
        alert('Failed to cancel job: ' + error.message);
    }
}

async function retryJob(jobId) {
    try {
        await fetchAPI(`/jobs/${jobId}/retry`, { method: 'POST' });
        loadData();
    } catch (error) {
        alert('Failed to retry job: ' + error.message);
    }
}

async function submitJob() {
    try {
        const job = {
            task: document.getElementById('task-name').value,
            args: JSON.parse(document.getElementById('task-args').value || '[]'),
            kwargs: JSON.parse(document.getElementById('task-kwargs').value || '{}'),
            queue: document.getElementById('task-queue').value || 'default',
            priority: parseInt(document.getElementById('task-priority').value) || 0,
        };

        await fetchAPI('/jobs', {
            method: 'POST',
            body: JSON.stringify(job),
        });

        newJobModal.classList.remove('active');
        newJobForm.reset();
        document.getElementById('task-args').value = '[]';
        document.getElementById('task-kwargs').value = '{}';
        document.getElementById('task-queue').value = 'default';
        document.getElementById('task-priority').value = '0';

        loadData();
    } catch (error) {
        alert('Failed to submit job: ' + error.message);
    }
}

// Worker Actions
async function scaleWorkers(count) {
    try {
        await fetchAPI('/workers/scale', {
            method: 'POST',
            body: JSON.stringify({ size: count }),
        });
        loadData();
    } catch (error) {
        alert('Failed to scale workers: ' + error.message);
    }
}

// DLQ Actions
async function replayDlqJob(jobId) {
    try {
        await fetchAPI(`/dlq/${jobId}/replay`, { method: 'POST' });
        loadData();
    } catch (error) {
        alert('Failed to replay job: ' + error.message);
    }
}

async function purgeDlqJob(jobId) {
    if (!confirm('Are you sure you want to permanently delete this job?')) {
        return;
    }

    try {
        await fetchAPI(`/dlq/${jobId}`, { method: 'DELETE' });
        loadData();
    } catch (error) {
        alert('Failed to purge job: ' + error.message);
    }
}

async function purgeDlq() {
    try {
        await fetchAPI('/dlq', { method: 'DELETE' });
        loadData();
    } catch (error) {
        alert('Failed to purge DLQ: ' + error.message);
    }
}

// Utilities
function formatDate(dateString) {
    if (!dateString) return '-';
    const date = new Date(dateString);
    return date.toLocaleString();
}
