let _toastTimeout = null;
function showToast(msg, type = "error") {
    let el = document.getElementById("toast");
    if (!el) {
        el = document.createElement("div");
        el.id = "toast";
        document.body.appendChild(el);
    }
    el.className = `toast ${type}`;
    el.textContent = msg;
    el.classList.add("visible");
    clearTimeout(_toastTimeout);
    _toastTimeout = setTimeout(() => el.classList.remove("visible"), 4000);
}

function h(tag, attrs, ...children) {
    const el = document.createElement(tag);
    for (const [k, v] of Object.entries(attrs || {})) {
        if (v == null || v === false) continue;
        if (k === "className") el.className = v;
        else if (k.startsWith("on") || k === "value" || k === "selected" || k === "disabled" || k === "checked") el[k] = v;
        else el.setAttribute(k, String(v));
    }
    for (const c of children) if (c != null && c !== false) el.append(c);
    return el;
}

function field(label, input) {
    return h("div", { className: "field" }, h("span", null, label), input);
}

function selectEl(className, options, selected) {
    return h("select", { className }, ...options.map(([val, text]) =>
        h("option", { value: val, selected: val === selected }, text)
    ));
}

function confirmDelete(name, callback) {
    const cancelBtn = h("button", { className: "small", onclick: () => dialog.remove() }, "Close");
    const deleteBtn = h("button", { className: "small danger", onclick: () => { dialog.remove(); callback(); } }, "Yes, delete it");
    const dialog = h("dialog", { className: "delete-popup" },
        h("span", { className: "popup-title" }, "Are you sure?"),
        h("div", { className: "popup-content" }, `Are you sure you want to delete "${name}"?`),
        h("div", { className: "popup-buttons" }, cancelBtn, deleteBtn),
    );
    dialog.onclick = (e) => { if (e.target === dialog) dialog.remove(); };
    document.body.appendChild(dialog);
    dialog.showModal();
}

function createChecksTable(tableId, slotId, sphereSidebar)
{
    const buildTableData = function (url, params, response) {
        let rows = [];

        // Populate sidebar
        if (sphereSidebar) {
            // Clear previous entries except the title
            while (sphereSidebar.children.length > 1) {
                sphereSidebar.removeChild(sphereSidebar.lastChild);
            }
            for (let i = 0; i < response.length; i++) {
                const sphere = response[i];
                const entry = document.createElement("div");
                entry.style.marginBottom = "0.25rem";
                entry.style.padding = "0.25rem";
                entry.style.borderBottom = "1px solid #444";

                const checkedCount = sphere.checked ?? 0;
                const total = sphere.total ?? sphere.locations.length;
                const allDone = checkedCount === total;

                entry.innerText = `Sphere ${i + 1}: ${checkedCount}/${total}`;
                entry.style.color = allDone ? "lightgreen" : "white";
                sphereSidebar.appendChild(entry);
            }
        }

        for (let i = 0; i < response.length; i++)
        {
            const sphere = response[i];
            for (const location of sphere.locations)
            {
                rows.push({
                    sphere: i + 1,
                    id: location[0],
                    location: location[1],
                    checked: location[2]
                });
            }
        }
        return rows;
    }

    const table = new Tabulator(tableId, {
        ajaxURL: `/api/dashboard/${window.lobby_room_id}/spheres/${slotId}`,
        ajaxResponse: buildTableData,
        height: "100%",
        layout: "fitDataStretch",
        initialSort: [
            { column: "location", dir: "asc"},
            { column: "sphere", dir: "asc" }
        ],
        columns: [
            { title: "Sphere", field: "sphere", sorter: "number" },
            { title: "Location", field: "location", headerFilter: "input" },
            { title: "Checked", field: "checked", formatter: "tickCross" }
        ]
    });

    return table;
}

function createAllChecksTable(tableId, sphereSidebar) {
    const buildTableData = function (url, params, response) {
        const rows = [];

        if (sphereSidebar) {
            while (sphereSidebar.children.length > 1) {
                sphereSidebar.removeChild(sphereSidebar.lastChild);
            }

            // Aggregate all slots' spheres by sphere index
            const sphereCount = Math.max(
                ...Object.values(response).map(spheres => spheres.length),
                0
            );

            for (let i = 0; i < sphereCount; i++) {
                let checked = 0;
                let total = 0;
                for (const spheres of Object.values(response)) {
                    if (i < spheres.length) {
                        checked += spheres[i].checked ?? 0;
                        total += spheres[i].total ?? spheres[i].locations.length;
                    }
                }

                const entry = document.createElement("div");
                entry.style.marginBottom = "0.25rem";
                entry.style.padding = "0.25rem";
                entry.style.borderBottom = "1px solid #444";
                entry.innerText = `Sphere ${i + 1}: ${checked}/${total}`;
                entry.style.color = checked === total ? "lightgreen" : "white";
                sphereSidebar.appendChild(entry);
            }
        }

        for (const [slotName, spheres] of Object.entries(response)) {
            for (let i = 0; i < spheres.length; i++) {
                for (const location of spheres[i].locations) {
                    rows.push({
                        sphere: i + 1,
                        slot: slotName,
                        id: location[0],
                        location: location[1],
                        checked: location[2],
                    });
                }
            }
        }
        return rows;
    };

    return new Tabulator(tableId, {
        ajaxURL: `/api/dashboard/${window.lobby_room_id}/spheres`,
        ajaxResponse: buildTableData,
        height: "100%",
        layout: "fitDataStretch",
        initialSort: [
            { column: "location", dir: "asc" },
            { column: "slot", dir: "asc" },
            { column: "sphere", dir: "asc" },
        ],
        columns: [
            { title: "Sphere",   field: "sphere", sorter: "number" },
            { title: "Slot",     field: "slot", headerFilter: "input" },
            { title: "Location", field: "location", headerFilter: "input" },
            { title: "Checked",  field: "checked",  formatter: "tickCross" },
        ],
    });
}


function createTrackerTable(tableId)
{
    const statusFormatter = function (cell, formatterParams) {
        const value = cell.getValue();
        return `<div class="slot-status slot-status-${value.replace(/\s/g, "")}"></span>`;
    }

    const checksFormatter = function (cell, formatterParams) {
        const values = cell.getValue();
        return `${values[0]} / ${values[1]}`;
    }

    const checksCalc = function (values, data, calcParams) {
        let totalFound = 0;
        let totalExisting = 0;

        for (const value of values) {
            totalFound += value[0];
            totalExisting += value[1];
        }

        return [totalFound, totalExisting];
    }

    const checksCalcFormatter = function (cell, formatterParams) {
        const values = cell.getValue();
        return `${values[0]} / ${values[1]}`;
    }

    const checksPercentFormatter = function (cell, formatterParams) {
        const values = cell.getValue();
        const percent = ((values[0] / values[1]) * 100).toFixed(1);
        return `${percent}%`;
    }

    const checksSorter = function (a, b) {
        return a[1] - b[1];
    }

    const checksPercentSorter = function (a, b) {
        return (a[0] / a[1]) - (b[0] / b[1]);
    }

    const lastActivityFormatter = function (cell, formatterParams) {
        const value = cell.getValue();
        const row = cell.getRow();
        const goaled = row.getData().status === "GoalCompleted";

        if (value === null)
        {
            return "Never";
        }
        let timeSinceClassname = "last-active-recent";
        if (!goaled) {
            if (value >= 3600) {
                timeSinceClassname = "last-active-hardbk";
            } else if (value >= 1800) {
                timeSinceClassname = "last-active-softbk";
            }
        }
        
        const text = timeSince(value) + " ago"
        return `<div class="${timeSinceClassname}">${text}</div>`;
    }

    const lastActivitySorter = function (a, b, aRow, bRow) {
        const aGoaled = aRow.getData().status === "GoalCompleted";
        const bGoaled = bRow.getData().status === "GoalCompleted";

        if (aGoaled !== bGoaled) {
            return aGoaled ? 1 : -1;
        }

        return a - b;
    }

    const onDiscordHandleClick = function (event, cell) {
        const row = cell.getRow();
        const discordId = row.getData().discord_id;

        navigator.clipboard.writeText(`<@${discordId}>`);
    }

    const table = new Tabulator(tableId, {
        ajaxURL: `/api/dashboard/${window.lobby_room_id}/tracker_info`,
        height: "100%",
        layout: "fitDataStretch",
        persistence: true,
        rowContextMenu: [
            {
                label: "View Checks",
                action: function (event, row) {
                    const { id, name } = row.getData();
                    openChecksTable(id, name);
                }
            },
            {
                label: "Password",
                action: function (event, row) {
                    const { id, lobby_slot_id, name } = row.getData();
                    getSlotPassword(id)
                    .then((password) => {
                        openPasswordPopup(lobby_slot_id, name, password);
                    })
                    .catch((err) => {
                        if (err?.status === 404) {
                            openPasswordPopup(lobby_slot_id, name, "None");
                        } else {
                            alert(err);
                        }
                    })
                }
            },
            {
                label: "Change Owner",
                action: async function (event, row) {
                    const { id, lobby_slot_id, name } = row.getData();
                    const password = await getSlotPassword(id)
                    .then((password) => {
                        return password;
                    })
                    .catch((err) => {
                        if (err?.status !== 404) {
                            alert(err)
                        }
                        return null;
                    });

                    openChangeOwnerPopup(lobby_slot_id, name, password);
                }
            },
            {
                label: "Copy Patch URL",
                disabled: function (component) {
                    return !component.getData().has_patch;
                },
                action: function (event, row) {
                    const { lobby_slot_id } = row.getData();
                    url = `${window.lobby_root_url}/room/${window.lobby_room_id}/patch/${lobby_slot_id}`;
                    navigator.clipboard.writeText(url);
                }
            },
            {
                label: "Toggle DeathBlock",
                action: function (event, row) {
                    const { id, name, game, deathlink_excluded } = row.getData();
                    openDeathBlock(id, name, game, deathlink_excluded);
                }
            },
            {
                label: "Goal Slot",
                action: function (event, row) {
                    const { name, game } = row.getData();
                    openRelease(name, game);
                }
            },
            {
                label: "Hint Item",
                action: function (event, row) {
                    const { name, game } = row.getData();
                    openAction("hint", name, game, "item");
                }
            },
            {
                label: "Give Item",
                action: function (event, row) {
                    const { name, game } = row.getData();
                    openAction("give", name, game, "item");
                }
            },
            {
                label: "Hint Location",
                action: function (event, row) {
                    const { name, game } = row.getData();
                    openAction("hint", name, game, "location");
                }
            },
            {
                label: "Give Location",
                action: function (event, row) {
                    const { name, game } = row.getData();
                    openAction("give", name, game, "location");
                }
            },
            {
                label: "Toggle Full Feed",
                action: function (event, row) {
                    const { id, name, game, full_feed } = row.getData();
                    openFullFeed(id, name, game, full_feed);
                }
            },
            {
                label: "Open Debug Viewer",
                action: function (event, row) {
                    const { id } = row.getData();
                    window.open(`/debug_slot/${id}`, '_blank');
                }
            }
        ],
        initialSort: [
            { column: "Name", dir:"asc" }
        ],
        columns: [
            { title: "Id", field: "id", sorter: "number" },
            { title: "S", field: "status", hozAlign: "center", formatter: statusFormatter },
            { title: "Name", field: "name", headerFilter: "input" },
            { title: "Game", field: "game", headerFilter:"list", headerFilterParams: { valuesLookup:true, clearable:true, sort: "asc" } },
            { title: "Checks", field: "checks", formatter: checksFormatter, sorter: checksSorter, bottomCalc: checksCalc, bottomCalcFormatter: checksCalcFormatter},
            { title: "Percent", field: "percent", mutator: function (value, data) {
                return data.checks;
            }, formatter: checksPercentFormatter, sorter: checksPercentSorter, bottomCalc: checksCalc, bottomCalcFormatter: checksPercentFormatter },
            { title: "Last Active", field: "last_activity", formatter: lastActivityFormatter, sorter: lastActivitySorter },
            { title: "Discord Handle", field: "discord_handle", cellClick: onDiscordHandleClick, headerFilter: "input" },
            { title: "S1", field: "incomplete_sphere1", mutator: function (value, data) {
                return !data.incomplete_sphere1;
            }, hozAlign: "center", formatter: "tickCross" }, 
            { title: "Deaths Allowed", field: "death_allowed", mutator: function (value, data) {
                return !data.deathlink_excluded;
            }, hozAlign: "center", formatter: "tickCross" },
            { title: "Deaths", field: "deathlinks_sent", bottomCalc: "sum" },
            { title: "FF", field: "full_feed", formatter: "tickCross", hozAlign: "center" },
        ]
    });

    table.on("dataLoaded", function (data) {
        console.log("data loaded");
        // Really shouldn't be using globals here, but eh
        window.review_data = data;
        if (window.slots_loaded_once !== true)
        {
            refreshSlotsToPing();
            // We need mutation to happen on first render. Should really move it out of the col setup?
            setTimeout(refreshSphere1ToPing, 200);
        }
    });

    window.review_table = table;

    setInterval(() => {
        table.replaceData(`/api/dashboard/${window.lobby_room_id}/tracker_info`);
    }, 40000);
}

function forceReviewTableRefresh() {
    if (window.review_table) {
        window.review_table.replaceData(`/api/dashboard/${window.lobby_room_id}/tracker_info`);
    }
}

function refreshSlotsToPing() {
    if (window.review_data !== undefined)
    {
        window.slots_loaded_once = true;
        // Get list of users with no activity
        const seen = new Set();
        const neverConnected = [];
        for (const row of window.review_data) {
            if (row["last_active"] == null && row["status"] == "Disconnected" && !seen.has(row["discord_id"])) {
                seen.add(row["discord_id"]);
                neverConnected.push([row["discord_handle"], row["discord_id"]]);
            }
        }

        const container = document.getElementById("unconnected-slots");

        // Remove old list
        container.querySelectorAll("ul").forEach(ul => ul.remove());

        // Build new list
        const ul = document.createElement("ul");

        // Chunk into groups
        for (let i = 0; i < neverConnected.length; i += 10) {
            const chunk = neverConnected.slice(i, i + 10);
            const mentions = chunk.map((val) => `<@${val[1]}>`).join(" ");
            const li = document.createElement("li");

            const button = document.createElement("button");
            button.style.marginLeft = "10px";
            button.style.cursor = "pointer";
            button.innerHTML = '<i class="fa-solid fa-copy"></i>';
            button.onclick = function () {
                navigator.clipboard.writeText(mentions + " you are not connected. If you need help please speak in the AP support channel. If you are connected in the meantime all good, you can ignore the ping");
            };
            li.appendChild(button);
    
            for (const val of chunk) {
                const span = document.createElement("span");
                span.textContent = "@" + val[0];
                li.appendChild(span);
            }
    
            ul.appendChild(li);
        }
    
        container.appendChild(ul);
    }
}

function refreshSphere1ToPing() {
    if (window.review_data === undefined) return;

    const seen = new Set();
    const incompleteSphere1 = [];

    for (const row of window.review_data) {
        if (
            // Cursed I know, but we flip it on first render
            row["incomplete_sphere1"] === false &&
            row["status"] !== "GoalCompleted" &&
            !seen.has(row["discord_id"])
        ) {
            seen.add(row["discord_id"]);
            incompleteSphere1.push([row["discord_handle"], row["discord_id"]]);
        }
    }

    const container = document.getElementById("sphere1-incomplete-slots");
    if (!container) return;

    // Remove old list
    container.querySelectorAll("ul").forEach(ul => ul.remove());

    // Build new list
    const ul = document.createElement("ul");

    for (let i = 0; i < incompleteSphere1.length; i += 10) {
        const chunk = incompleteSphere1.slice(i, i + 10);
        const mentions = chunk.map((val) => `<@${val[1]}>`).join(" ");
        const li = document.createElement("li");

        const button = document.createElement("button");
        button.style.marginLeft = "10px";
        button.style.cursor = "pointer";
        button.innerHTML = '<i class="fa-solid fa-copy"></i>';
        button.onclick = function () {
            navigator.clipboard.writeText(
                mentions + " you have checks you haven’t done that have been available since the start, please do them :)"
            );
        };
        li.appendChild(button);

        for (const val of chunk) {
            const span = document.createElement("span");
            span.textContent = "@" + val[0];
            li.appendChild(span);
        }

        ul.appendChild(li);
    }

    container.appendChild(ul);
}

function timeSince(secondsSince) {
    const seconds = Math.floor(secondsSince);
  
    let interval = seconds / 31536000;
  
    if (interval > 1) {
      return Math.floor(interval) + " years";
    }
    interval = seconds / 2592000;
    if (interval > 1) {
      return Math.floor(interval) + " months";
    }
    interval = seconds / 86400;
    if (interval > 1) {
      return Math.floor(interval) + " days";
    }
    interval = seconds / 3600;
    if (interval > 1) {
      return Math.floor(interval) + " hours";
    }
    interval = seconds / 60;
    if (interval > 1) {
      return Math.floor(interval) + " minutes";
    }
    return Math.floor(seconds) + " seconds";
}

async function getSlotPassword(slotId) {
    const res = await fetch("/api/dashboard/" + window.lobby_room_id + "/password/" + slotId);
    if (res.ok) {
        return (await res.json())["password"];
    }
    const body = await res.json().catch(() => ({}));
    const err = new Error(`HTTP ${res.status}: ${body.message ?? JSON.stringify(body)}`);
    err.status = res.status;
    throw err;
}