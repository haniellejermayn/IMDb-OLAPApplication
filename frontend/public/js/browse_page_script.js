document.addEventListener("DOMContentLoaded", () => {
    const filterButton = document.getElementById("filter_button");
    const peopleContainer = document.getElementById("people_included_container");
    const personInput = document.getElementById("person_input");

    // === 🧍 Add person on Enter key ===
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

    // === 🧩 Function to add person tags ===
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

    // === 🎬 Apply Filters ===
    filterButton.addEventListener("click", () => {
        // ✅ Required: Report
        const report = document.querySelector("input[name='report']:checked")?.value;
        if (!report) {
            alert("Please select a report!");
            return;
        }

        // Optional filters
        const genres = Array.from(document.querySelectorAll("input[name='genre']:checked")).map(el => el.value);

        const minRating = document.getElementById("min_rating")?.value || "";
        const minRuntime = document.getElementById("min_runtime_val")?.value || "";
        const maxRuntime = document.getElementById("max_runtime_val")?.value || "";

        const status = document.querySelector("input[name='status']:checked")?.value || "";

        const people = Array.from(document.querySelectorAll("#people_included_container .people_included span"))
            .map(el => el.textContent);

        // Optional: grouping fields (Roll Up / Drill Down)
        const groupFields = Array.from(document.querySelectorAll("input[name='field']:checked")).map(el => el.value);

        console.log("=== Filter Values ===");
        console.log("Report:", report);
        console.log("Genres:", genres);
        console.log("Min Rating:", minRating);
        console.log("Runtime:", `${minRuntime} - ${maxRuntime}`);
        console.log("Status:", status);
        console.log("People Included:", people);
        console.log("Group By:", groupFields);

        alert("Filters applied! Check console for details.");
    });

    // === 🔄 Reset Filters ===
    const resetButton = document.getElementById("reset_button");

    resetButton.addEventListener("click", () => {
        // Clear genres
        document.querySelectorAll("input[name='genre']").forEach(cb => cb.checked = false);

        // Clear rating, runtime, and person input
        ["min_rating", "min_runtime_val", "max_runtime_val", "person_input"].forEach(id => {
            const el = document.getElementById(id);
            if (el) el.value = "";
        });

        // Clear people tags
        peopleContainer.innerHTML = "";

        // Clear status
        ["ongoing", "complete"].forEach(id => {
            const el = document.getElementById(id);
            if (el) el.checked = false;
        });

        // Clear grouping fields
        document.querySelectorAll("input[name='field']").forEach(cb => cb.checked = false);

        // Clear report selection
        document.querySelectorAll("input[name='report']").forEach(rb => rb.checked = false);

        console.log("All filters reset!");
    });

    // === Show/hide sections based on selected report === // 
    const reportRadios = document.querySelectorAll(".report_select");
    const allFilterFields = document.querySelectorAll("#filter_row .filter_field, #filter_row .filter_field_dual .filter_field");
    const allGroupFields = document.querySelectorAll(".agg_fields .field");

    allFilterFields.forEach(f => f.style.display = "none");
    allGroupFields.forEach(f => f.style.display = "none");

    const reportConfig = {
        R1: {
            filters: ["#genre_fields", "#year_range_field", "#min_rating_field"],
            groups: ["#genre_agg", "#time_agg"]
        },
        R2: {
            filters: ["#season_number_field", "#min_votes_field"],
            groups: ["#tv_content_agg", "#time_agg"]
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
