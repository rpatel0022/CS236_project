// State management
let appState = {
    selectedDataset: null,
    availableDatasets: [],
    availableColumns: [],
    filters: [],
    currentPage: 1,
    limit: 50,
    results: null
};

// Initialize app on page load
document.addEventListener('DOMContentLoaded', function() {
    console.log('Hotel Reservations WebUI loaded');
    initApp();
});

async function initApp() {
    await loadDatasets();
    renderUI();
}

// Fetch available datasets from the database
async function loadDatasets() {
    try {
        const response = await fetch('/datasets');
        const data = await response.json();
        appState.availableDatasets = data.datasets;
    } catch (error) {
        console.error('Error loading datasets:', error);
        showError('Failed to load datasets from database');
    }
}

// Fetch columns for selected dataset
async function loadColumns(dataset) {
    try {
        const response = await fetch(`/columns/${dataset}`);
        const data = await response.json();
        appState.availableColumns = data.columns;
        renderFilters();
    } catch (error) {
        console.error('Error loading columns:', error);
        showError('Failed to load columns for dataset');
    }
}

// Main UI render function
function renderUI() {
    const app = document.getElementById('app');
    app.innerHTML = `
        <div class="container mt-4">
            <div class="row mb-4">
                <div class="col-12">
                    <h1 class="display-5 mb-3">Hotel Reservations Database Explorer</h1>
                    <p class="text-muted">Search and filter hotel booking data</p>
                </div>
            </div>

            <!-- Dataset Selection -->
            <div class="row mb-4">
                <div class="col-md-6">
                    <div class="card shadow-sm">
                        <div class="card-body">
                            <h5 class="card-title"><i class="bi bi-database"></i> Select Dataset</h5>
                            <select id="dataset-select" class="form-select form-select-lg">
                                <option value="">Choose a dataset...</option>
                                ${appState.availableDatasets.map(ds => 
                                    `<option value="${ds}">${formatDatasetName(ds)}</option>`
                                ).join('')}
                            </select>
                        </div>
                    </div>
                </div>
                <div class="col-md-6">
                    <div class="card shadow-sm">
                        <div class="card-body">
                            <h5 class="card-title"><i class="bi bi-gear"></i> Query Settings</h5>
                            <div class="d-flex align-items-center gap-3">
                                <label for="limit-select" class="form-label mb-0">Results per page:</label>
                                <select id="limit-select" class="form-select" style="width: auto;">
                                    <option value="25">25</option>
                                    <option value="50" selected>50</option>
                                    <option value="100">100</option>
                                    <option value="200">200</option>
                                </select>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Filters Section -->
            <div class="row mb-4" id="filters-section" style="display: none;">
                <div class="col-12">
                    <div class="card shadow-sm">
                        <div class="card-body">
                            <div class="d-flex justify-content-between align-items-center mb-3">
                                <h5 class="card-title mb-0"><i class="bi bi-funnel"></i> Filters</h5>
                                <button id="add-filter-btn" class="btn btn-primary btn-sm">
                                    <i class="bi bi-plus-circle"></i> Add Filter
                                </button>
                            </div>
                            <div id="filters-container"></div>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Query Actions -->
            <div class="row mb-4" id="query-actions" style="display: none;">
                <div class="col-12">
                    <div class="d-flex gap-2">
                        <button id="query-btn" class="btn btn-success btn-lg">
                            <i class="bi bi-search"></i> Execute Query
                        </button>
                        <button id="clear-btn" class="btn btn-outline-secondary btn-lg">
                            <i class="bi bi-x-circle"></i> Clear All
                        </button>
                    </div>
                </div>
            </div>

            <!-- Loading Indicator -->
            <div id="loading" class="row mb-4" style="display: none;">
                <div class="col-12">
                    <div class="alert alert-info">
                        <div class="spinner-border spinner-border-sm me-2" role="status"></div>
                        Loading data...
                    </div>
                </div>
            </div>

            <!-- Error Messages -->
            <div id="error-container"></div>

            <!-- Results Section -->
            <div id="results-section" class="row" style="display: none;">
                <div class="col-12">
                    <div class="card shadow-sm">
                        <div class="card-body">
                            <div class="d-flex justify-content-between align-items-center mb-3">
                                <h5 class="card-title mb-0"><i class="bi bi-table"></i> Results</h5>
                                <div id="results-info"></div>
                            </div>
                            <div class="table-responsive">
                                <table id="results-table" class="table table-striped table-hover">
                                </table>
                            </div>
                            <div id="pagination-container" class="d-flex justify-content-center mt-3"></div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    `;

    attachEventListeners();
}

// Attach event listeners
function attachEventListeners() {
    // Dataset selection
    document.getElementById('dataset-select').addEventListener('change', handleDatasetChange);
    
    // Limit selection
    document.getElementById('limit-select').addEventListener('change', function() {
        appState.limit = parseInt(this.value);
        appState.currentPage = 1;
    });

    // Add filter button
    const addFilterBtn = document.getElementById('add-filter-btn');
    if (addFilterBtn) {
        addFilterBtn.addEventListener('click', addFilter);
    }

    // Query button
    const queryBtn = document.getElementById('query-btn');
    if (queryBtn) {
        queryBtn.addEventListener('click', executeQuery);
    }

    // Clear button
    const clearBtn = document.getElementById('clear-btn');
    if (clearBtn) {
        clearBtn.addEventListener('click', clearAll);
    }
}

// Handle dataset change
async function handleDatasetChange(event) {
    const dataset = event.target.value;
    if (!dataset) {
        document.getElementById('filters-section').style.display = 'none';
        document.getElementById('query-actions').style.display = 'none';
        document.getElementById('results-section').style.display = 'none';
        return;
    }

    appState.selectedDataset = dataset;
    appState.filters = [];
    appState.currentPage = 1;
    appState.results = null;

    await loadColumns(dataset);
    
    document.getElementById('filters-section').style.display = 'block';
    document.getElementById('query-actions').style.display = 'block';
    document.getElementById('results-section').style.display = 'none';
}

// Render filters
function renderFilters() {
    const container = document.getElementById('filters-container');
    
    if (appState.filters.length === 0) {
        container.innerHTML = '<p class="text-muted mb-0">No filters applied. Click "Add Filter" to start filtering data.</p>';
        return;
    }

    container.innerHTML = appState.filters.map((filter, index) => `
        <div class="filter-row mb-3 p-3 border rounded bg-light">
            <div class="row g-2">
                <div class="col-md-3">
                    <label class="form-label small">Column</label>
                    <select class="form-select filter-column" data-index="${index}">
                        ${appState.availableColumns.map(col => 
                            `<option value="${col}" ${filter.column === col ? 'selected' : ''}>${col}</option>`
                        ).join('')}
                    </select>
                </div>
                <div class="col-md-3">
                    <label class="form-label small">Operator</label>
                    <select class="form-select filter-operator" data-index="${index}">
                        <option value="equals" ${filter.operator === 'equals' ? 'selected' : ''}>Equals</option>
                        <option value="contains" ${filter.operator === 'contains' ? 'selected' : ''}>Contains</option>
                        <option value="greater" ${filter.operator === 'greater' ? 'selected' : ''}>Greater Than</option>
                        <option value="less" ${filter.operator === 'less' ? 'selected' : ''}>Less Than</option>
                    </select>
                </div>
                <div class="col-md-5">
                    <label class="form-label small">Value</label>
                    <input type="text" class="form-control filter-value" data-index="${index}" value="${filter.value || ''}" placeholder="Enter value...">
                </div>
                <div class="col-md-1 d-flex align-items-end">
                    <button class="btn btn-danger btn-sm w-100 remove-filter-btn" data-index="${index}">
                        <i class="bi bi-trash"></i>
                    </button>
                </div>
            </div>
        </div>
    `).join('');

    // Attach filter event listeners
    document.querySelectorAll('.filter-column').forEach(el => {
        el.addEventListener('change', updateFilter);
    });
    document.querySelectorAll('.filter-operator').forEach(el => {
        el.addEventListener('change', updateFilter);
    });
    document.querySelectorAll('.filter-value').forEach(el => {
        el.addEventListener('input', updateFilter);
    });
    document.querySelectorAll('.remove-filter-btn').forEach(el => {
        el.addEventListener('click', removeFilter);
    });
}

// Add new filter
function addFilter() {
    if (appState.availableColumns.length === 0) return;
    
    appState.filters.push({
        column: appState.availableColumns[0],
        operator: 'equals',
        value: ''
    });
    renderFilters();
}

// Update filter
function updateFilter(event) {
    const index = parseInt(event.target.dataset.index);
    const filterType = event.target.classList.contains('filter-column') ? 'column' :
                      event.target.classList.contains('filter-operator') ? 'operator' : 'value';
    
    appState.filters[index][filterType] = event.target.value;
}

// Remove filter
function removeFilter(event) {
    const index = parseInt(event.currentTarget.dataset.index);
    appState.filters.splice(index, 1);
    renderFilters();
}

// Clear all
function clearAll() {
    appState.filters = [];
    appState.currentPage = 1;
    appState.results = null;
    renderFilters();
    document.getElementById('results-section').style.display = 'none';
    document.getElementById('error-container').innerHTML = '';
}

// Execute query
async function executeQuery() {
    if (!appState.selectedDataset) {
        showError('Please select a dataset first');
        return;
    }

    // Filter out empty values
    const validFilters = appState.filters.filter(f => f.value !== '');

    const queryData = {
        dataset: appState.selectedDataset,
        filters: validFilters,
        limit: appState.limit,
        page: appState.currentPage
    };

    // Show loading
    document.getElementById('loading').style.display = 'block';
    document.getElementById('results-section').style.display = 'none';
    document.getElementById('error-container').innerHTML = '';

    try {
        const response = await fetch('/query', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(queryData)
        });

        const data = await response.json();

        if (data.success) {
            appState.results = data;
            renderResults();
        } else {
            showError(data.error || 'Query failed');
        }
    } catch (error) {
        console.error('Query error:', error);
        showError('Failed to execute query: ' + error.message);
    } finally {
        document.getElementById('loading').style.display = 'none';
    }
}

// Render results
function renderResults() {
    const resultsSection = document.getElementById('results-section');
    const resultsTable = document.getElementById('results-table');
    const resultsInfo = document.getElementById('results-info');
    const paginationContainer = document.getElementById('pagination-container');

    if (!appState.results || !appState.results.data || appState.results.data.length === 0) {
        resultsSection.style.display = 'block';
        resultsTable.innerHTML = '<p class="text-muted">No results found</p>';
        return;
    }

    const { data, columns, count } = appState.results;

    // Results info
    const start = (appState.currentPage - 1) * appState.limit + 1;
    const end = start + count - 1;
    resultsInfo.innerHTML = `<span class="badge bg-primary">${count} rows (Page ${appState.currentPage})</span>`;

    // Build table
    let tableHTML = '<thead class="table-dark"><tr>';
    columns.forEach(col => {
        tableHTML += `<th>${col}</th>`;
    });
    tableHTML += '</tr></thead><tbody>';

    data.forEach(row => {
        tableHTML += '<tr>';
        columns.forEach(col => {
            const value = row[col];
            const displayValue = value === null ? '<em class="text-muted">null</em>' : value;
            tableHTML += `<td>${displayValue}</td>`;
        });
        tableHTML += '</tr>';
    });
    tableHTML += '</tbody>';

    resultsTable.innerHTML = tableHTML;

    // Pagination
    const hasMore = count === appState.limit;
    paginationContainer.innerHTML = `
        <div class="btn-group">
            <button class="btn btn-outline-primary" id="prev-page" ${appState.currentPage === 1 ? 'disabled' : ''}>
                <i class="bi bi-chevron-left"></i> Previous
            </button>
            <button class="btn btn-outline-primary disabled">Page ${appState.currentPage}</button>
            <button class="btn btn-outline-primary" id="next-page" ${!hasMore ? 'disabled' : ''}>
                Next <i class="bi bi-chevron-right"></i>
            </button>
        </div>
    `;

    // Pagination event listeners
    document.getElementById('prev-page')?.addEventListener('click', () => {
        if (appState.currentPage > 1) {
            appState.currentPage--;
            executeQuery();
        }
    });

    document.getElementById('next-page')?.addEventListener('click', () => {
        if (hasMore) {
            appState.currentPage++;
            executeQuery();
        }
    });

    resultsSection.style.display = 'block';
}

// Show error message
function showError(message) {
    const errorContainer = document.getElementById('error-container');
    errorContainer.innerHTML = `
        <div class="alert alert-danger alert-dismissible fade show" role="alert">
            <i class="bi bi-exclamation-triangle"></i> ${message}
            <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
        </div>
    `;
}

// Format dataset name for display
function formatDatasetName(name) {
    // Convert snake_case to Title Case
    return name.split('_').map(word => 
        word.charAt(0).toUpperCase() + word.slice(1)
    ).join(' ');
}
  