document.addEventListener("DOMContentLoaded", () => {
    const filterButton = document.getElementById("filter_button");
    const peopleContainer = document.getElementById("people_included_container");
    const personInput = document.getElementById("person_input");

    var payload = {}

    // Shown fields based on report
    const reportConfig = {
        R1: {
            filters: ["#genre_fields", "#year_range_field", "#min_rating_field"],
            groups: ["#genre_agg", "#time_agg"]
        },
        R2: {
            filters: ["#genre_fields", "#min_rating_field", "#title_type_field", "#year_range_field", "#min_votes_field", "#rating_range_field"],
            groups: ["#genre_agg", "#time_agg"]
        },
        R3: {
            filters: ["#genre_fields", "#runtime_field", "#completion_status_field"],
            groups: ["#genre_agg"]
        },
        R4: {
            filters: ["#people_included_field", "#year_range_field"],
            groups: ["#actor_agg", "#time_agg"]
        },
        R5: {
            filters: ["#genre_fields", "#min_rating_field", "#title_type_field", "#year_range_field", "#min_votes_field", "#completion_status_field", "#series_name_field"],
            groups: ["#genre_agg", "#tv_content_agg", "#time_agg"]
        }
    };

    // === Add person on Enter key ===
    if (personInput) {
        personInput.addEventListener("keydown", (e) => {
            if (e.key === "Enter") {
                e.preventDefault();
                const name = personInput.value.trim();
                if (name) {
                    addPerson(name);
                    personInput.value = "";
                }
            }
        });
    }

    // === Function to add person tags ===
    function addPerson(name) {
        const div = document.createElement("div");
        div.classList.add("people_included");

        const span = document.createElement("span");
        span.textContent = name;

        const removeIcon = document.createElement("img");
        removeIcon.src = "/public/resources/remove_icon.svg";
        removeIcon.alt = "Remove Icon";
        removeIcon.classList.add("remove_icon");

        removeIcon.addEventListener("click", () => div.remove());

        div.appendChild(span);
        div.appendChild(removeIcon);
        peopleContainer.appendChild(div);
    }

    // === Apply Filters ===
    filterButton.addEventListener("click", () => {
        const report = document.querySelector("input[name='report']:checked")?.value;
        if (!report) {
            alert("Please select a report!");
            return;
        }

        const config = reportConfig[report];
        if (!config) {
            console.error("No configuration found for report:", report);
            return;
        }

        // Get Values
        const filtersData = {};

        for (const selector of config.filters) {
            const el = document.querySelector(selector);
            if (!el) continue;

            const inputs = el.querySelectorAll("input, select, textarea");

            inputs.forEach(input => {
                const id = input.id || input.name || selector; 
                if (input.type === "checkbox") {
                    if (input.checked) {
                        const key = input.name || id;

                        if (!filtersData[key]) filtersData[key] = [];
                        filtersData[key].push(input.value);
                    }
                } else if (input.type === "radio") {
                    if (input.checked) filtersData[input.name] = input.value;
                } else {
                    filtersData[id] = input.value;
                }
            });
        }

        const groupData = {};

        for (const selector of config.groups) {
            const el = document.querySelector(selector);
            if (!el) continue;

            const checkboxes = el.querySelectorAll("input[type='checkbox']");
            checkboxes.forEach(cb => {
                if (cb.checked) {
                    const key = cb.name || cb.id || "group_field";
                    if (!groupData[key]) groupData[key] = [];
                    groupData[key].push(cb.value);
                }
            });

            const select = el.querySelector("select");
            if (select && select.value.trim() !== "") {
                groupData[select.name || select.id] = select.value;
            }
        }

        payload = {
            report,
            filters: filtersData,
            groups: groupData
        };

        const checked = validateData();

        console.log("=== Report Submission ===");
        console.log("Selected Report:", report);
        console.log("Filters:", filtersData);
        console.log("Groups:", groupData);

        if (checked) {
            sendData(payload);
            alert("Filters applied! Check console for details.");
        }
    });

    function validateData() {
        report = ""
        check = true

        if (payload.report == "R2") {
            if (payload.groups.time == "" || payload.groups.time == null) {
                report += "Please aggregate by time.\n"
                check = false
            }

            if (payload.filters.min_rating && (payload.filters.min_rating_val || payload.filters.max_rating_val)) {
                report += "Rating range will be given priority.\n"
                payload.filters.min_rating = ""
                
                if (!payload.filters.max_rating_val || !payload.filters.min_rating_val)
                    report += "Please complete the rating range values.\n"

                check = false
            }

            if (!check)
                alert(report)
        }

        return check
    }

    async function sendData() {
        const routeMap = {
            R1: "/genre_analysis",
            R2: "/api/olap/runtime_trend",
            R3: "/completion_status",
            R4: "/actor_performance",
            R5: "/title_hierarchy"
        };

        const endpoint = routeMap[payload.report];
        if (!endpoint) {
            console.error("❌ No backend route defined for this report:", payload.report);
            alert("No backend route defined for this report.");
            return;
        }

        try {
            console.log(`Sending data to ${endpoint} ...`);
            console.log("Payload:", payload);

            const response = await fetch(endpoint, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payload)
            });

            if (!response.ok) {
                throw new Error(`HTTP error! Status: ${response.status}`);
            }

            const data = await response.json();
            console.log(`Response from ${endpoint}:`, data);

            alert(`Data received from ${endpoint}! Check console for details.`);

            // Render Chart
            // renderChart(data.results);

            // Render Table
            renderTable(data.results)

            // Set Query and Param
            document.getElementById("queryBox").textContent = data.query || "No query returned";
            document.getElementById("paramsBox").textContent = JSON.stringify(data.params || [], null, 2);

        } catch (error) {
            console.error("❌ Error sending data to backend:", error);
            alert("Failed to send data to backend. Check console for details.");
        }
    }

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
    
    // === Reset Filters ===
    const resetButton = document.getElementById("reset_button");

    resetButton.addEventListener("click", () => {
        document.querySelectorAll("input[name='genre']").forEach(cb => cb.checked = false);

        ["min_rating", "min_runtime_val", "max_runtime_val", "person_input"].forEach(id => {
            const el = document.getElementById(id);
            if (el) el.value = "";
        });

        peopleContainer.innerHTML = "";

        ["ongoing", "complete"].forEach(id => {
            const el = document.getElementById(id);
            if (el) el.checked = false;
        });

        document.querySelectorAll("input[name='field']").forEach(cb => cb.checked = false);

        document.querySelectorAll("input[name='report']").forEach(rb => rb.checked = false);

        console.log("All filters reset!");
    });

    // === Show/hide sections based on selected report === // 
    const reportRadios = document.querySelectorAll(".report_select");
    const allFilterFields = document.querySelectorAll("#filter_row .filter_field, #filter_row .filter_field_dual .filter_field");
    const allGroupFields = document.querySelectorAll(".agg_fields .field");

    allFilterFields.forEach(f => f.style.display = "none");
    allGroupFields.forEach(f => f.style.display = "none");

    reportRadios.forEach(radio => {
        radio.addEventListener("change", () => {
            allFilterFields.forEach(f => f.style.display = "none");
            allGroupFields.forEach(f => f.style.display = "none");

            const config = reportConfig[radio.value];
            if (!config) return;

            config.filters.forEach(sel => {
                const el = document.querySelector(sel);
                if (el) el.style.display = "";
            });

            config.groups.forEach(sel => {
                const el = document.querySelector(sel);
                if (el) {
                    el.style.display = "";
                    const parent = el.closest(".agg_fields"); 
                    if (parent) parent.style.display = ""; 
                }
            });
        });
    });

    const tvContentSelect = document.querySelector("#tv_content_level");
    const seasonNumberField = document.querySelector("#season_number_field");

    tvContentSelect.addEventListener("change", () => {
        const selectedReport = [...reportRadios].find(r => r.checked)?.value;

        if (selectedReport === "R5") {
            if (tvContentSelect.value === "Episode") {
                seasonNumberField.style.display = "";
            } else {
                seasonNumberField.style.display = "none";
            }
        }
    });

    if (resetButton) {
        resetButton.addEventListener("click", () => {
            allFilterFields.forEach(f => f.style.display = "none");
            allGroupFields.forEach(f => f.style.display = "none");
            reportRadios.forEach(r => (r.checked = false));
        });
    }
});

document.addEventListener("DOMContentLoaded", function() {
    const tableContainer = document.getElementById("resultsTableContainer");
    const chartContainer = document.getElementById("chartContainer");
    const viewTableLink = document.getElementById("viewTableLink");
    const viewChartLink = document.getElementById("viewChartLink");

    // Default: show table
    tableContainer.style.display = "block";
    chartContainer.style.display = "none";

    // When "View Table" is clicked
    viewTableLink.addEventListener("click", function(e) {
        e.preventDefault();
        tableContainer.style.display = "block";
        chartContainer.style.display = "none";
        viewTableLink.style.fontWeight = "bold";
        viewChartLink.style.fontWeight = "normal";
    });

    // When "View Chart" is clicked
    viewChartLink.addEventListener("click", function(e) {
        e.preventDefault();
        tableContainer.style.display = "none";
        chartContainer.style.display = "block";
        viewChartLink.style.fontWeight = "bold";
        viewTableLink.style.fontWeight = "normal";
    });
});
