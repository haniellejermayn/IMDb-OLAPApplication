let currentChart = null;
let currentReport = null;

// Report selection
function selectReport(report) {
     currentReport = report;
     
     // Hide all forms
     document.querySelectorAll('.form-container').forEach(form => {
          form.classList.add('hidden');
     });
     
     // Update button states
     document.querySelectorAll('.report-btn').forEach(btn => {
          btn.classList.remove('active');
     });
     event.target.classList.add('active');
     
     // Show selected form
     document.getElementById(`form${report}`).classList.remove('hidden');
     
     // Hide status message
     document.getElementById('statusMessage').classList.add('hidden');
     
     // Hide chi-square results when switching reports
     document.getElementById('chiSquareResults').classList.add('hidden');
}

// Helper function to show status messages
function showStatus(message, isError = false) {
     const statusEl = document.getElementById('statusMessage');
     statusEl.textContent = message;
     statusEl.className = isError ? 'error' : 'success';
     statusEl.classList.remove('hidden');
}

// Helper to parse comma-separated values
function parseArray(value) {
     if (!value || value.trim() === '') return null;
     return value.split(',').map(v => v.trim());
}

// R1 Data Fetcher
async function fetchR1Data() {
     const payload = {
          time_granularity: document.getElementById('r1_time_granularity').value
     };

     if (!payload.time_granularity) {
          showStatus('Time Granularity is required for R1!', true);
          return;
     }

     // Optional filters
     const genres = parseArray(document.getElementById('r1_genres').value);
     if (genres) payload.genres = genres;
     
     const titleTypes = parseArray(document.getElementById('r1_title_types').value);
     if (titleTypes) payload.title_types = titleTypes;
     
     const startYear = document.getElementById('r1_start_year').value;
     if (startYear) payload.start_year = parseInt(startYear);
     
     const endYear = document.getElementById('r1_end_year').value;
     if (endYear) payload.end_year = parseInt(endYear);
     
     const minRating = document.getElementById('r1_min_rating').value;
     if (minRating) payload.min_rating = parseFloat(minRating);
     
     const minVotes = document.getElementById('r1_min_votes').value;
     if (minVotes) payload.min_votes = parseInt(minVotes);
     
     const runtimeMin = document.getElementById('r1_runtime_min').value;
     if (runtimeMin) payload.runtime_min = parseInt(runtimeMin);
     
     const runtimeMax = document.getElementById('r1_runtime_max').value;
     if (runtimeMax) payload.runtime_max = parseInt(runtimeMax);

     const calculateChi = document.getElementById('r1_calculate_chi_square').value;
     if (calculateChi === 'true') payload.calculate_chi_square = true;

     await fetchData('/api/reports/r1', payload, 'R1');
}

// R2 Data Fetcher
async function fetchR2Data() {
     const payload = {
          time_granularity: document.getElementById('r2_time_granularity').value
     };

     if (!payload.time_granularity) {
          showStatus('Time Granularity is required for R2!', true);
          return;
     }

     const genres = parseArray(document.getElementById('r2_genres').value);
     if (genres) payload.genres = genres;
     
     const titleTypes = parseArray(document.getElementById('r2_title_types').value);
     if (titleTypes) payload.title_types = titleTypes;
     
     const startYear = document.getElementById('r2_start_year').value;
     if (startYear) payload.start_year = parseInt(startYear);
     
     const endYear = document.getElementById('r2_end_year').value;
     if (endYear) payload.end_year = parseInt(endYear);
     
     const minRating = document.getElementById('r2_min_rating').value;
     if (minRating) payload.min_rating = parseFloat(minRating);
     
     const minVotes = document.getElementById('r2_min_votes').value;
     if (minVotes) payload.min_votes = parseInt(minVotes);
     
     const runtimeMin = document.getElementById('r2_runtime_min').value;
     if (runtimeMin) payload.runtime_min = parseInt(runtimeMin);
     
     const runtimeMax = document.getElementById('r2_runtime_max').value;
     if (runtimeMax) payload.runtime_max = parseInt(runtimeMax);

     await fetchData('/api/reports/r2', payload, 'R2');
}

// R3 Data Fetcher
async function fetchR3Data() {
     const payload = {
          job_category: document.getElementById('r3_job_category').value
     };

     if (!payload.job_category) {
          showStatus('Job Category is required for R3!', true);
          return;
     }

     // Get the additional group by value
     const groupBy = document.getElementById('r3_group_by').value;
     if (groupBy === 'genre') {
          payload.group_by_genre = true;
     } else if (groupBy.startsWith('time_')) {
          const granularity = groupBy.split('_')[1];
          payload.group_by_time = true;
          payload.time_granularity = granularity;
     }

     const genres = parseArray(document.getElementById('r3_genres').value);
     if (genres) payload.genres = genres;
     
     const titleTypes = parseArray(document.getElementById('r3_title_types').value);
     if (titleTypes) payload.title_types = titleTypes;
     
     const startYear = document.getElementById('r3_start_year').value;
     if (startYear) payload.start_year = parseInt(startYear);
     
     const endYear = document.getElementById('r3_end_year').value;
     if (endYear) payload.end_year = parseInt(endYear);
     
     const minRating = document.getElementById('r3_min_rating').value;
     if (minRating) payload.min_rating = parseFloat(minRating);
     
     const minVotes = document.getElementById('r3_min_votes').value;
     if (minVotes) payload.min_votes = parseInt(minVotes);
     
     const minTitles = document.getElementById('r3_min_titles').value;
     if (minTitles) payload.min_titles = parseInt(minTitles);

     await fetchData('/api/reports/r3', payload, 'R3');
}

// R4 Data Fetcher
async function fetchR4Data() {
     const payload = {};

     // Get the additional group by value
     const groupBy = document.getElementById('r4_group_by').value;
     if (groupBy.startsWith('time_')) {
          const granularity = groupBy.split('_')[1];
          payload.time_granularity = granularity;
     }

     const genres = parseArray(document.getElementById('r4_genres').value);
     if (genres) payload.genres = genres;
     
     const titleTypes = parseArray(document.getElementById('r4_title_types').value);
     if (titleTypes) payload.title_types = titleTypes;
     
     const startYear = document.getElementById('r4_start_year').value;
     if (startYear) payload.start_year = parseInt(startYear);
     
     const endYear = document.getElementById('r4_end_year').value;
     if (endYear) payload.end_year = parseInt(endYear);
     
     const minRating = document.getElementById('r4_min_rating').value;
     if (minRating) payload.min_rating = parseFloat(minRating);
     
     const voteMin = document.getElementById('r4_vote_min').value;
     if (voteMin) payload.vote_min = parseInt(voteMin);
     
     const voteMax = document.getElementById('r4_vote_max').value;
     if (voteMax) payload.vote_max = parseInt(voteMax);

     await fetchData('/api/reports/r4', payload, 'R4');
}

// R5 Data Fetcher
async function fetchR5Data() {
     const payload = {
          tv_level: document.getElementById('r5_tv_level').value
     };

     if (!payload.tv_level) {
          showStatus('TV Level is required for R5!', true);
          return;
     }

     // Get the additional group by value
     const groupBy = document.getElementById('r5_group_by').value;
     if (groupBy === 'genre') {
          payload.group_by_genre = true;
     } else if (groupBy.startsWith('time_')) {
          const granularity = groupBy.split('_')[1];
          payload.group_by_time = true;
          payload.time_granularity = granularity;
     }

     const genres = parseArray(document.getElementById('r5_genres').value);
     if (genres) payload.genres = genres;
     
     const titleTypes = parseArray(document.getElementById('r5_title_types').value);
     if (titleTypes) payload.title_types = titleTypes;
     
     const startYear = document.getElementById('r5_start_year').value;
     if (startYear) payload.start_year = parseInt(startYear);
     
     const endYear = document.getElementById('r5_end_year').value;
     if (endYear) payload.end_year = parseInt(endYear);
     
     const minRating = document.getElementById('r5_min_rating').value;
     if (minRating) payload.min_rating = parseFloat(minRating);
     
     const minVotes = document.getElementById('r5_min_votes').value;
     if (minVotes) payload.min_votes = parseInt(minVotes);
     
     const completionStatus = document.getElementById('r5_completion_status').value;
     if (completionStatus) payload.completion_status = completionStatus;
     
     const seriesName = document.getElementById('r5_series_name').value;
     if (seriesName) payload.series_name = seriesName;
     
     const seasonNumber = document.getElementById('r5_season_number').value;
     if (seasonNumber) payload.season_number = parseInt(seasonNumber);

     await fetchData('/api/reports/r5', payload, 'R5');
}

// Generic fetch function
async function fetchData(endpoint, payload, reportType) {
     try {
          showStatus('Fetching data...', false);
          console.log(`Sending request to ${endpoint}:`, payload);

          const response = await fetch(endpoint, {
               method: 'POST',
               headers: { 'Content-Type': 'application/json' },
               body: JSON.stringify(payload)
          });

          const result = await response.json();
          console.log('Response:', result);

          if (result.status === 'success' && result.data) {
               showStatus(`✓ Successfully fetched ${result.data.length} rows`, false);

               // Render charts and table
               renderChart(result.data, reportType);
               renderTable(result.data);

               // Show query and parameter generated
               document.getElementById("queryBox").innerHTML = 
                    `SQL Query Generated:<pre><code>${result.query || "No query returned"}</code></pre>`;

               document.getElementById("paramsBox").innerHTML = 
                    `Parameters:<pre><code>${JSON.stringify(result.params || [], null, 2)}</code></pre>`;

               
               // Display chi-square results if present (R1 report)
               if (result.chi_square_analysis) {
                    displayChiSquareResults(result.chi_square_analysis);
               } else {
                    // Hide chi-square section if not present
                    document.getElementById('chiSquareResults').classList.add('hidden');
               }
          } 
          else {
               showStatus('Error: ' + (result.message || 'Unknown error'), true);
          }
     } catch (error) {
          console.error('Fetch error:', error);
          showStatus('Failed to fetch data: ' + error.message, true);
     }
}

// Display Chi-Square Analysis Results
function displayChiSquareResults(chiData) {
     const container = document.getElementById('chiSquareResults');
     const content = document.getElementById('chiSquareContent');
     
     if (!chiData || chiData.error) {
          container.classList.add('hidden');
          return;
     }
     
     let html = `
          <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin-bottom: 20px;">
          <div style="background: white; padding: 15px; border-radius: 6px; border: 1px solid #ffc107;">
               <div style="font-size: 12px; color: #856404; margin-bottom: 5px;">Chi-Square Statistic</div>
               <div style="font-size: 24px; font-weight: bold; color: #333;">${chiData.chi_square_statistic}</div>
          </div>
          <div style="background: white; padding: 15px; border-radius: 6px; border: 1px solid #ffc107;">
               <div style="font-size: 12px; color: #856404; margin-bottom: 5px;">Degrees of Freedom</div>
               <div style="font-size: 24px; font-weight: bold; color: #333;">${chiData.degrees_of_freedom}</div>
          </div>
          <div style="background: white; padding: 15px; border-radius: 6px; border: 1px solid #ffc107;">
               <div style="font-size: 12px; color: #856404; margin-bottom: 5px;">Critical Value (α=0.05)</div>
               <div style="font-size: 24px; font-weight: bold; color: #333;">${chiData['critical_value_alpha_0.05']}</div>
          </div>
          <div style="background: white; padding: 15px; border-radius: 6px; border: 1px solid #ffc107;">
               <div style="font-size: 12px; color: #856404; margin-bottom: 5px;">Significance</div>
               <div style="font-size: 18px; font-weight: bold; color: ${chiData.is_significant ? '#28a745' : '#dc3545'};">
                    ${chiData.is_significant ? '✓ Significant' : '✗ Not Significant'}
               </div>
          </div>
          </div>
          
          <div style="background: white; padding: 15px; border-radius: 6px; margin-bottom: 15px; border: 1px solid #ffc107;">
          <div style="font-weight: bold; margin-bottom: 8px; color: #856404;">Interpretation:</div>
          <div style="color: #333;">${chiData.interpretation}</div>
          </div>
          
          <div style="background: white; padding: 15px; border-radius: 6px; margin-bottom: 15px; border: 1px solid #ffc107;">
          <div style="font-weight: bold; margin-bottom: 8px; color: #856404;">Summary Statistics:</div>
          <div style="color: #333;">
               <strong>Grand Total:</strong> ${chiData.grand_total.toLocaleString()} observations<br>
          </div>
          </div>
     `;
     
     // Top contributions table
     if (chiData.top_contributions && chiData.top_contributions.length > 0) {
          html += `
          <div style="background: white; padding: 15px; border-radius: 6px; border: 1px solid #ffc107;">
               <div style="font-weight: bold; margin-bottom: 10px; color: #856404;">Top 10 Cell Contributions to Chi-Square:</div>
               <div style="overflow-x: auto;">
                    <table style="width: 100%; border-collapse: collapse; font-size: 13px;">
                         <thead>
                              <tr style="background: #f8f9fa; border-bottom: 2px solid #dee2e6;">
                              <th style="padding: 8px; text-align: left;">Genre</th>
                              <th style="padding: 8px; text-align: left;">Rating Bin</th>
                              <th style="padding: 8px; text-align: right;">Observed</th>
                              <th style="padding: 8px; text-align: right;">Expected</th>
                              <th style="padding: 8px; text-align: right;">Contribution</th>
                              </tr>
                         </thead>
                         <tbody>
          `;
          
          chiData.top_contributions.forEach((contrib, idx) => {
          html += `
               <tr style="border-bottom: 1px solid #dee2e6; ${idx % 2 === 0 ? 'background: #f8f9fa;' : ''}">
                    <td style="padding: 8px;">${contrib.genre}</td>
                    <td style="padding: 8px;">${contrib.rating_bin}</td>
                    <td style="padding: 8px; text-align: right;">${contrib.observed}</td>
                    <td style="padding: 8px; text-align: right;">${contrib.expected}</td>
                    <td style="padding: 8px; text-align: right; font-weight: bold; color: ${contrib.contribution > 10 ? '#dc3545' : '#333'};">${contrib.contribution}</td>
               </tr>
          `;
          });
          
          html += `
                         </tbody>
                    </table>
               </div>
               <div style="margin-top: 10px; font-size: 12px; color: #666;">
                    <em>Note: Higher contribution values indicate cells that deviate most from expected values under independence.</em>
               </div>
          </div>
          `;
     }
     
     content.innerHTML = html;
     container.classList.remove('hidden');
}

// Main render table function
function renderTable(results) {
     console.log("Rendering table with results:", results);

     if (!results || results.length === 0) {
          console.warn("⚠️ No results to display.");
          return;
     }

     // Destroy existing DataTable (if any)
     if ($.fn.DataTable.isDataTable('#resultsTable')) {
          $('#resultsTable').DataTable().destroy();
     }

     // Clear table
     $('#resultsHeader').empty();
     $('#resultsBody').empty();

     // Get column names dynamically from first object
     const columns = Object.keys(results[0]);

     // Build table header
     columns.forEach(col => {
          $('#resultsHeader').append(`<th>${col}</th>`);
     });

     // Build table rows
     results.forEach(row => {
          const rowHtml = columns.map(col => `<td>${row[col]}</td>`).join('');
          $('#resultsBody').append(`<tr>${rowHtml}</tr>`);
     });

     // Initialize DataTable
     $('#resultsTable').DataTable({
          pageLength: 50,      
          lengthChange: false,  
          ordering: true,       
          searching: false,      
          paging: true,         
          scrollX: true,        
          responsive: true,
          autoWidth: false,     
          columnDefs: [
               { width: 'auto', targets: '_all' } 
          ],
          language: {
               paginate: { previous: "←", next: "→" },
               info: "Showing _START_ to _END_ of _TOTAL_ entries"
          }
     });
}

// Sample data generators (for testing)
function generateR1Data() {
     return [
          { genre: 'Action', rating_bin: 'Low', time_period: 2020, count: 150 },
          { genre: 'Action', rating_bin: 'Mid', time_period: 2020, count: 450 },
          { genre: 'Action', rating_bin: 'High', time_period: 2020, count: 200 },
          { genre: 'Drama', rating_bin: 'Low', time_period: 2020, count: 100 },
          { genre: 'Drama', rating_bin: 'Mid', time_period: 2020, count: 400 },
          { genre: 'Drama', rating_bin: 'High', time_period: 2020, count: 300 },
          { genre: 'Action', rating_bin: 'Low', time_period: 2021, count: 120 },
          { genre: 'Action', rating_bin: 'Mid', time_period: 2021, count: 480 },
          { genre: 'Action', rating_bin: 'High', time_period: 2021, count: 250 },
     ];
}

function generateR2Data() {
     return [
          { time_period: 2018, titleType: 'movie', avg_runtime: 110.5 },
          { time_period: 2019, titleType: 'movie', avg_runtime: 115.2 },
          { time_period: 2020, titleType: 'movie', avg_runtime: 118.8 },
          { time_period: 2021, titleType: 'movie', avg_runtime: 120.1 },
          { time_period: 2018, titleType: 'tvSeries', avg_runtime: 42.3 },
          { time_period: 2019, titleType: 'tvSeries', avg_runtime: 43.1 },
          { time_period: 2020, titleType: 'tvSeries', avg_runtime: 44.5 },
          { time_period: 2021, titleType: 'tvSeries', avg_runtime: 45.2 },
     ];
}

function generateR3Data() {
     const names = ['Christopher Nolan', 'Steven Spielberg', 'Quentin Tarantino', 
                    'Martin Scorsese', 'James Cameron', 'Ridley Scott'];
     return names.map((name, i) => ({
          nconst: `nm${i}`,
          primaryName: name,
          avg_rating: 8.5 - (i * 0.3),
          total_titles: 20 - (i * 2)
     }));
}

function generateR4Data() {
     const genres = ['Action', 'Drama', 'Comedy', 'Thriller', 'Sci-Fi'];
     return genres.map(genre => ({
          genreName: genre,
          total_votes: Math.floor(Math.random() * 500000) + 100000,
          title_count: Math.floor(Math.random() * 100) + 50,
          avg_votes_per_title: Math.floor(Math.random() * 5000) + 1000,
          avg_rating: (Math.random() * 3 + 6).toFixed(1)
     }));
}

function generateR5Data() {
     return [
          { series_title: 'Breaking Bad', total_votes: 1500000, avg_rating: 9.5, episode_count: 62 },
          { series_title: 'Game of Thrones', total_votes: 1800000, avg_rating: 9.2, episode_count: 73 },
          { series_title: 'The Office', total_votes: 1200000, avg_rating: 8.9, episode_count: 201 },
          { series_title: 'Friends', total_votes: 1000000, avg_rating: 8.8, episode_count: 236 },
     ];
}

// Test functions
function testR1Chart() {
     const data = generateR1Data();
     console.log('Testing R1 with data:', data);
     showStatus('Loaded sample R1 data', false);
     renderChart(data, 'R1');
}

function testR2Chart() {
     const data = generateR2Data();
     console.log('Testing R2 with data:', data);
     showStatus('Loaded sample R2 data', false);
     renderChart(data, 'R2');
}

function testR3Chart() {
     const data = generateR3Data();
     console.log('Testing R3 with data:', data);
     showStatus('Loaded sample R3 data', false);
     renderChart(data, 'R3');
}

function testR4Chart() {
     const data = generateR4Data();
     console.log('Testing R4 with data:', data);
     showStatus('Loaded sample R4 data', false);
     renderChart(data, 'R4');
}

function testR5Chart() {
     const data = generateR5Data();
     console.log('Testing R5 with data:', data);
     showStatus('Loaded sample R5 data', false);
     renderChart(data, 'R5');
}

// Main render table function
/*function renderGroupedTable(container, results, reportType, groupByField) {
     const groupedData = {};
     
     // Group results by the specified field
     results.forEach(row => {
          const groupKey = row[groupByField] || 'N/A';
          if (!groupedData[groupKey]) {
          groupedData[groupKey] = [];
          }
          groupedData[groupKey].push(row);
     });
     
     // Determine column headers based on report type
     let columns = [];
     if (reportType === 'R2') {
          columns = ['time_period', 'titleType', 'avg_runtime', 'title_count', 'avg_rating'];
     } else if (reportType === 'R3') {
          columns = ['primaryName', 'avg_rating', 'total_titles'];
     } else if (reportType === 'R4') {
          columns = ['time_period', 'total_votes', 'title_count', 'avg_votes_per_title', 'avg_rating'];
     } else if (reportType === 'R5') {
          // Check what level of data we have
          const hasEpisodeNumber = results[0]?.episodeNumber !== undefined;
          const hasSeasonNumber = results[0]?.seasonNumber !== undefined;
          
          if (hasEpisodeNumber && hasSeasonNumber) {
          // Episode level
          columns = ['series_title', 'seasonNumber', 'episodeNumber', 'total_votes', 'avg_rating'];
          } else if (hasSeasonNumber) {
          // Season level
          columns = ['series_title', 'seasonNumber', 'episode_count', 'total_votes', 'avg_rating'];
          } else {
          // Series level - check if we have season/episode counts
          const hasSeasonCount = results[0]?.season_count !== undefined;
          if (hasSeasonCount) {
               columns = ['series_title', 'season_count', 'episode_count', 'total_votes', 'avg_rating'];
          } else {
               columns = ['series_title', 'episode_count', 'total_votes', 'avg_rating'];
          }
          }
     }
     
     const columnLabels = {
          'time_period': 'Time Period',
          'titleType': 'Title Type',
          'avg_runtime': 'Avg Runtime (min)',
          'title_count': 'Title Count',
          'avg_rating': 'Avg Rating',
          'primaryName': 'Person',
          'total_titles': 'Total Titles',
          'total_votes': 'Total Votes',
          'avg_votes_per_title': 'Avg Votes/Title',
          'series_title': 'Series Title',
          'episode_count': 'Episodes',
          'season_count': 'Seasons',
          'seasonNumber': 'Season',
          'episodeNumber': 'Episode'
     };
     
     let html = '<div style="display: grid; gap: 30px;">';
     let totalGroups = Object.keys(groupedData).length;
     let groupCount = 0;
     
     Object.entries(groupedData).forEach(([groupName, data]) => {
          groupCount++;
          // Limit to top 10 per group and sort by most relevant metric
          let topData = data.slice(0, 10);
          
          html += `
          <div style="border: 1px solid #ddd; padding: 20px; border-radius: 8px; background: white;">
               <h3 style="margin-bottom: 5px; color: #333; font-size: 18px;">
                    ${groupName} <span style="font-size: 14px; color: #999; font-weight: normal;">([${groupCount}/${totalGroups}])</span>
               </h3>
               <p style="margin: 0 0 15px 0; font-size: 12px; color: #666;">Showing top ${topData.length} of ${data.length}</p>
               <div style="overflow-x: auto;">
                    <table style="width: 100%; border-collapse: collapse; font-size: 13px;">
                         <thead>
                              <tr style="background: #f8f9fa; border-bottom: 2px solid #dee2e6;">
          `;
          
          columns.forEach(col => {
          html += `<th style="padding: 10px; text-align: left; font-weight: 600; white-space: nowrap;">${columnLabels[col] || col}</th>`;
          });
          
          html += `
                              </tr>
                         </thead>
                         <tbody>
          `;
          
          topData.forEach((row, idx) => {
          html += `<tr style="border-bottom: 1px solid #eee; ${idx % 2 === 0 ? 'background: #fafafa;' : ''}">`;
          
          columns.forEach(col => {
               let value = row[col];
               
               // Special formatting for season and episode numbers
               if (col === 'seasonNumber' && value !== undefined && value !== null) {
                    value = `S${String(value).padStart(2, '0')}`;
               } else if (col === 'episodeNumber' && value !== undefined && value !== null) {
                    value = `E${String(value).padStart(2, '0')}`;
               }
               
               // Format numeric values
               if (typeof value === 'number') {
                    if (col.includes('rating')) {
                         value = value.toFixed(2);
                    } else if (col.includes('avg') && col.includes('runtime')) {
                         value = value.toFixed(1);
                    } else if (col.includes('votes') || col.includes('count')) {
                         value = Math.round(value).toLocaleString();
                    } else if (col.includes('titles')) {
                         value = Math.round(value);
                    }
               }
               html += `<td style="padding: 10px;">${value || 'N/A'}</td>`;
          });
          
          html += `</tr>`;
          });
          
          html += `
                         </tbody>
                    </table>
               </div>
          </div>
          `;
     });
     
     html += '</div>';
     container.innerHTML = html;
}*/

// Main render chart function
function renderChart(results, reportType) {
     console.log("Rendering chart for report:", reportType, results);
     
     if (!results || results.length === 0) {
          console.warn("⚠️ No results to display in chart.");
          document.getElementById('chartContainer').innerHTML = '<p style="padding: 20px; text-align: center; color: #999;">No data available to display.</p>';
          return;
     }

     if (currentChart) {
          currentChart.destroy();
          currentChart = null;
     }

     const chartContainer = document.getElementById('chartContainer');
     
     // Check what grouping fields exist in the data
     const hasGenre = results[0]?.genreName !== undefined;
     const hasTimePeriod = results[0]?.time_period !== undefined;
     const hasTitleType = results[0]?.titleType !== undefined;
     
     console.log(`R${reportType.slice(1)} - hasGenre: ${hasGenre}, hasTimePeriod: ${hasTimePeriod}, hasTitleType: ${hasTitleType}`);
     
     // Use table view if optional grouping is present
     /*if (reportType === 'R3' && hasGenre) {
          renderGroupedTable(chartContainer, results, reportType, 'genreName');
          return;
     }
     if (reportType === 'R3' && hasTimePeriod) {
          renderGroupedTable(chartContainer, results, reportType, 'time_period');
          return;
     }
     if (reportType === 'R4' && hasTimePeriod) {
          renderGroupedTable(chartContainer, results, reportType, 'time_period');
          return;
     }
     if (reportType === 'R5' && hasGenre) {
          renderGroupedTable(chartContainer, results, reportType, 'genreName');
          return;
     }
     if (reportType === 'R5' && hasTimePeriod) {
          renderGroupedTable(chartContainer, results, reportType, 'time_period');
          return;
     }*/
     
     // Otherwise render chart as normal
     chartContainer.innerHTML = '<canvas id="resultsChart"></canvas>';
     const ctx = document.getElementById('resultsChart').getContext('2d');

     switch(reportType) {
          case 'R1':
          renderR1Chart(ctx, results);
          break;
          case 'R2':
          renderR2Chart(ctx, results);
          break;
          case 'R3':
          renderR3Chart(ctx, results);
          break;
          case 'R4':
          renderR4Chart(ctx, results);
          break;
          case 'R5':
          renderR5Chart(ctx, results);
          break;
          default:
          console.error("Unknown report type:", reportType);
     }
}

// R1: Genre-Rating Association Chart
function renderR1Chart(ctx, results) {
     const timePeriods = [...new Set(results.map(r => r.time_period))].sort();
     const ratingBins = ['Very Low', 'Low', 'Mid', 'High', 'Very High'];
     
     const datasets = ratingBins.map(bin => {
          const data = timePeriods.map(period => {
               const entry = results.find(r => r.time_period === period && r.rating_bin === bin);
               return entry ? entry.count : 0;
          });
          const colors = {
               'Very Low': 'rgba(255, 99, 132, 0.7)',
               'Low': 'rgba(255, 206, 86, 0.7)',
               'Mid': 'rgba(75, 192, 192, 0.7)',
               'High': 'rgba(153, 102, 255, 0.7)',
               'Very High': 'rgba(255, 159, 64, 0.7)'
          };
          return {
               label: `${bin} Rating`,
               data: data,
               backgroundColor: colors[bin],
               borderColor: colors[bin].replace('0.7', '1'),
               borderWidth: 1
          };
     });

     currentChart = new Chart(ctx, {
          type: 'bar',
          data: {
               labels: timePeriods,
               datasets: datasets
          },
          options: {
               responsive: true,
               maintainAspectRatio: false,
               plugins: {
                    title: {
                         display: true,
                         text: 'Genre-Rating Distribution Over Time',
                         font: { size: 16 }
                    },
                    legend: {
                         display: true,
                         position: 'top'
                    }
               },
               scales: {
                    x: {
                         stacked: true,
                         title: { display: true, text: 'Time Period' }
                    },
                    y: {
                         stacked: true,
                         title: { display: true, text: 'Count' },
                         beginAtZero: true
                    }
               }
          }
     });
}

// R2: Runtime Trends Chart
function renderR2Chart(ctx, results) {
     const timePeriods = [...new Set(results.map(r => r.time_period))].sort();
     const titleTypes = [...new Set(results.map(r => r.titleType))];
     
     const colorPalette = [
          'rgba(54, 162, 235, 0.7)',
          'rgba(255, 99, 132, 0.7)',
          'rgba(75, 192, 192, 0.7)',
          'rgba(153, 102, 255, 0.7)',
          'rgba(255, 159, 64, 0.7)'
     ];

     const datasets = titleTypes.map((type, index) => {
          const data = timePeriods.map(period => {
          const entry = results.find(r => r.time_period === period && r.titleType === type);
          return entry ? parseFloat(entry.avg_runtime) : null;
          });
          return {
          label: type,
          data: data,
          borderColor: colorPalette[index % colorPalette.length],
          backgroundColor: colorPalette[index % colorPalette.length].replace('0.7', '0.2'),
          borderWidth: 2,
          fill: false,
          tension: 0.4
          };
     });

     currentChart = new Chart(ctx, {
          type: 'line',
          data: {
          labels: timePeriods,
          datasets: datasets
          },
          options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
               title: {
                    display: true,
                    text: 'Average Runtime Trends Over Time',
                    font: { size: 16 }
               },
               legend: {
                    display: true,
                    position: 'top'
               }
          },
          scales: {
               x: {
                    title: { display: true, text: 'Time Period' }
               },
               y: {
                    title: { display: true, text: 'Average Runtime (minutes)' },
                    beginAtZero: true
               }
          }
          }
     });
}

// R3: Person Performance Chart
function renderR3Chart(ctx, results) {
     const topResults = results
          .sort((a, b) => parseFloat(b.avg_rating) - parseFloat(a.avg_rating))
          .slice(0, 10);
     
     const labels = topResults.map(r => r.primaryName || r.nconst);
     const ratings = topResults.map(r => parseFloat(r.avg_rating));
     const titleCounts = topResults.map(r => parseInt(r.total_titles));

     currentChart = new Chart(ctx, {
          type: 'bar',
          data: {
          labels: labels,
          datasets: [
               {
                    label: 'Average Rating',
                    data: ratings,
                    backgroundColor: 'rgba(54, 162, 235, 0.7)',
                    borderColor: 'rgba(54, 162, 235, 1)',
                    borderWidth: 1,
                    yAxisID: 'y'
               },
               {
                    label: 'Total Titles',
                    data: titleCounts,
                    backgroundColor: 'rgba(255, 159, 64, 0.7)',
                    borderColor: 'rgba(255, 159, 64, 1)',
                    borderWidth: 1,
                    yAxisID: 'y1'
               }
          ]
          },
          options: {
          indexAxis: 'y',
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
               title: {
                    display: true,
                    text: 'Top 10 Person Performance by Average Rating',
                    font: { size: 16 }
               },
               legend: {
                    display: true,
                    position: 'top'
               }
          },
          scales: {
               x: {
                    title: { display: true, text: 'Value' }
               },
               y: {
                    position: 'left',
                    title: { display: true, text: 'Average Rating' }
               },
               y1: {
                    position: 'right',
                    title: { display: true, text: 'Total Titles' },
                    grid: { drawOnChartArea: false }
               }
          }
          }
     });
}

// R4: Genre Engagement Chart
function renderR4Chart(ctx, results) {
     const hasTimePeriod = results[0]?.time_period !== undefined;
     
     if (hasTimePeriod) {
          const timePeriods = [...new Set(results.map(r => r.time_period))].sort();
          const genres = [...new Set(results.map(r => r.genreName))];
          
          const colorPalette = [
          'rgba(255, 99, 132, 0.7)',
          'rgba(54, 162, 235, 0.7)',
          'rgba(255, 206, 86, 0.7)',
          'rgba(75, 192, 192, 0.7)',
          'rgba(153, 102, 255, 0.7)'
          ];

          const datasets = genres.slice(0, 10).map((genre, index) => {
          const data = timePeriods.map(period => {
               const entry = results.find(r => r.time_period === period && r.genreName === genre);
               return entry ? parseInt(entry.total_votes) : 0;
          });
          return {
               label: genre,
               data: data,
               backgroundColor: colorPalette[index % colorPalette.length],
               borderColor: colorPalette[index % colorPalette.length].replace('0.7', '1'),
               borderWidth: 1
          };
          });

          currentChart = new Chart(ctx, {
          type: 'bar',
          data: {
               labels: timePeriods,
               datasets: datasets
          },
          options: {
               responsive: true,
               maintainAspectRatio: false,
               plugins: {
                    title: {
                         display: true,
                         text: 'Genre Engagement by Time Period',
                         font: { size: 16 }
                    },
                    legend: {
                         display: true,
                         position: 'top'
                    }
               },
               scales: {
                    x: {
                         title: { display: true, text: 'Time Period' }
                    },
                    y: {
                         title: { display: true, text: 'Total Votes' },
                         beginAtZero: true
                    }
               }
          }
          });
     } else {
          const sortedResults = results
          .sort((a, b) => parseInt(b.total_votes) - parseInt(a.total_votes))
          .slice(0, 10);
          
          const labels = sortedResults.map(r => r.genreName);
          const votes = sortedResults.map(r => parseInt(r.total_votes));

          currentChart = new Chart(ctx, {
          type: 'bar',
          data: {
               labels: labels,
               datasets: [{
                    label: 'Total Votes',
                    data: votes,
                    backgroundColor: 'rgba(75, 192, 192, 0.7)',
                    borderColor: 'rgba(75, 192, 192, 1)',
                    borderWidth: 1
               }]
          },
          options: {
               responsive: true,
               maintainAspectRatio: false,
               plugins: {
                    title: {
                         display: true,
                         text: 'Top 10 Genres by Total Engagement',
                         font: { size: 16 }
                    },
                    legend: { display: false }
               },
               scales: {
                    x: {
                         title: { display: true, text: 'Genre' }
                    },
                    y: {
                         title: { display: true, text: 'Total Votes' },
                         beginAtZero: true
                    }
               }
          }
          });
     }
}

// R5: TV Series Engagement Chart
function renderR5Chart(ctx, results) {
     const topResults = results
          .sort((a, b) => parseInt(b.total_votes) - parseInt(a.total_votes))
          .slice(0, 10);
     
     let labels;
     if (topResults[0].hasOwnProperty('episode_title')) {
          // Episode level - show series, season, and episode number
          labels = topResults.map(r => {
          const season = String(r.seasonNumber).padStart(2, '0');
          const episode = String(r.episodeNumber).padStart(2, '0');
          return `${r.series_title} S${season}E${episode}`;
          });
     } else if (topResults[0].hasOwnProperty('seasonNumber')) {
          // Season level - show series and season
          labels = topResults.map(r => `${r.series_title} S${String(r.seasonNumber).padStart(2, '0')}`);
     } else {
          // Series level
          labels = topResults.map(r => r.series_title);
     }

     const votes = topResults.map(r => parseInt(r.total_votes));
     const ratings = topResults.map(r => parseFloat(r.avg_rating));

     currentChart = new Chart(ctx, {
          type: 'bar',
          data: {
          labels: labels,
          datasets: [
               {
                    label: 'Total Votes',
                    data: votes,
                    backgroundColor: 'rgba(54, 162, 235, 0.7)',
                    borderColor: 'rgba(54, 162, 235, 1)',
                    borderWidth: 1,
                    yAxisID: 'y'
               },
               {
                    label: 'Average Rating',
                    data: ratings,
                    backgroundColor: 'rgba(255, 206, 86, 0.7)',
                    borderColor: 'rgba(255, 206, 86, 1)',
                    borderWidth: 1,
                    yAxisID: 'y1'
               }
          ]
          },
          options: {
          indexAxis: 'y',
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
               title: {
                    display: true,
                    text: 'Top 10 TV Content by Engagement',
                    font: { size: 16 }
               },
               legend: {
                    display: true,
                    position: 'top'
               }
          },
          scales: {
               x: {
                    title: { display: true, text: 'Value' }
               },
               y: {
                    position: 'left',
                    title: { display: true, text: 'Total Votes' }
               },
               y1: {
                    position: 'right',
                    title: { display: true, text: 'Average Rating' },
                    max: 10,
                    grid: { drawOnChartArea: false }
               }
          }
          }
     });
}

// Auto-load a test chart on page load
window.addEventListener('DOMContentLoaded', () => {
     testR2Chart();
});