// browse_page.js

document.addEventListener("DOMContentLoaded", () => {
     const filterButton = document.getElementById("filter_button");
     const peopleContainer = document.getElementById("people_included_container");
     const personInput = document.getElementById("person_input");

     // === 🧍 Add person on Enter key ===
     if (personInput) {
          personInput.addEventListener("keydown", (e) => {
               // Use "keydown" instead of "keypress" (better browser support)
               if (e.key === "Enter") {
               e.preventDefault(); // prevent accidental form submit
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

          // Remove on click
          removeIcon.addEventListener("click", () => div.remove());

          div.appendChild(span);
          div.appendChild(removeIcon);
          peopleContainer.appendChild(div);
     }

     // === 🎬 Apply Filters ===
     filterButton.addEventListener("click", () => {
          const genres = Array.from(
               document.querySelectorAll("input[name='genre']:checked")
          ).map((el) => el.value);

          const minRating = document.getElementById("min_rating")?.value || "";
          const minRuntime = document.getElementById("min_runtime_val")?.value || "";
          const maxRuntime = document.getElementById("max_runtime_val")?.value || "";
          const status = document.querySelector("input[name='status']:checked")?.value;

          const people = Array.from(
               document.querySelectorAll("#people_included_container .people_included span")
          ).map((el) => el.textContent);

          console.log("=== Filter Values ===");
          console.log("Genres:", genres);
          console.log("Min Rating:", minRating);
          console.log("Runtime:", `${minRuntime} - ${maxRuntime}`);
          console.log("Status:", status);
          console.log("People Included:", people);

          alert("Filters applied! Check console for details.");
     });

     const resetButton = document.getElementById("reset_button");

     resetButton.addEventListener("click", () => {
          document.querySelectorAll("input[name='genre']").forEach((checkbox) => {
               checkbox.checked = false;
          });

          // 2. Clear rating and runtime inputs
          const inputsToClear = [
               "min_rating",
               "min_runtime_val",
               "max_runtime_val",
               "person_input"
          ];
          inputsToClear.forEach((id) => {
               const el = document.getElementById(id);
               if (el) el.value = "";
          });

          const peopleContainer = document.getElementById("people_included_container");
          peopleContainer.innerHTML = "";

          const ongoing_stat = document.getElementById("ongoing");
          ongoing_stat.checked = false;
          const completed_stat = document.getElementById("complete");
          completed_stat.checked = false;

          console.log("All filters reset!");
     });
});
