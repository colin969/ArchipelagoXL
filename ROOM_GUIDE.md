# Contents

- Creating a [lobby room](#lobby) and collecting yamls
- [Generating](#generation) a game from a lobby room
- [Hosting](#hosting) a game for your lobby room
- Using the [Helper Dashboard](#helper-dashboard)

# Lobby

To open a new lobby room, click on `Create new room` on the left sidebar of the website. If this isn't there, you may need to be whitelisted by a server admin.

Enter your room name, the time you want submissions to close, and a description. Leave Room URL blank.

You can turn off worlds you don't want to support under `Apworlds`

Under `Advanced Options` you have checkboxes to:
- Validate uploaded YAML files
  - This will only works for worlds in the index. Yamls will show as Green (validated), Yellow (unsupported) or Red (invalid)
- Allow YAMLs with worlds not supported by the lobby
- Allow invalid YAMLs to be uploaded

- Limit the number of games a user can submit
- Keep YAMLs bundled on upload
  - Recommended to be off
- Lock room (prevent YAML edits and deletions)
  - Once uploaded, users cannot change their submissions

# Generation

Once you've collected your yamls in your lobby, you can either generate on-site, or provide your own generation file.

For on-site generation:
- All YAMLs files must have been validated
-	The room must be closed
- The room must contain between 1 and 100 YAMLs
- There must be no generation in progress

Whilst your own generation can be uploaded without these conditions met, it is **highly recommended** that the room be closed first.

In order to use the dashboard later, your own generation **must** also have been done with the same player list as was submitted to the room.

After generation is finished, you can either choose to host on-site by navigating to the `Host` tab, or download your generation file and host elsewhere.

# Hosting

If hosting off-site, you can leave the guide now.

For on-site hosting, make sure you've [generated](#generation) your game on-site, or uploaded it, and then head to the `Host` tab on the lobby room page.

The [Dashboard](#helper-dashboard) can be used after a hosted room is made. Only the room creator can access this.

Here you will have a few different options:
- Per-slot passwords
  - Players will not be able to connect to a slot without it's password.
  - These passwords must be generated via the Dashboard, until then nobody can access the slot.
- Disable deathlink
  - By default, all players will have a bounce tag exclusion preventing their DeathLink packets reaching other slots
  - This exclusion can be removed per-player via the Dashboard
- Reduced access
  - By default, all players will not be able to access the Normal address, and instead only the Reduced address.
    - This limits Print messages unrelated to their slot from being sent to their client
  - This exclusion can be removed per-player via the Dashboard

Once the room is running, these options cannot be changed until the room is closed.

After it's started, you will see both the Normal Addr, and the Reduced Addr on the host page.
You will also be assigned an admin password for the room (for !admin commands in a text client), which will work regardless of whether you set a password during generation or not. This cannot currently be changed without deleting the room.
Players in the lobby room will see the these server addresses, a download to any patch files they need, and their per-slot password if enabled.

Stopping / closing a room will save the progress. Only the host can re-open it via the host page. This will also happen after a 2 hour timeout with no activity.

Deleting a room is unrecoverable. 

# Helper Dashboard

The dashboard is only available if your generation used the same yamls that are present in the lobby.

## Room Actions

Above the review table you can find a few different actions:
- Ionium Lobby
  - Opens the Ionium lobby room page in a new tab
- Yaml Review
  - **Currently for server teams only** - Opens the yaml review tool for the lobby room in a new tab
- Deathlink Probability
  - Get and set the deathlink probability between 0 and 1. (0% and 100%). 0.5 for example will ignore 50% of deathlink packets sent.
- View All Checks
  - Opens 1 big table with all the checks in the game, what sphere they're in, whose slot they're at, and whether they've been found. Can take a few seconds to load.
- Reset Table
  - Resets the persistent values of the table (e.g column width), useful if something seems to break visually

## Review Table

The table displays the current status of every slot in the archipelago:

- ID
- State - (D)isconnected, (P)laying, (G)oaled
- Name
- Game
- Checks
  - Sorts by total, not number found, use percentage for that instead
- Percent - Checks as a percentage
- Sphere 1 - Whether they've done all Sphere 1 checks
- Last Active - Time since last new check found
- Discord Handle
  - Click to copy their id as a mention
- Deaths Allowed - Whether DeathLinks will be dropped by the server if they send them
- Deaths - DeathLinks sent, including those dropped
- Isolated - Prevents all bounce packets being sent to or received from other slots
- Normal Access - Whether they can connect to the 'Normal Addr' listed on the Host page

The calculations for checks, percentage and deaths at the bottom of the table will be affected by your filter options

Number of goaled slots is displayed underneath the table

### Filter Options

Name, Game and Discord Handle can be filtered via their table headers.

Hide goaled slots is an additional checkbox above the table, this filter will be applied on top of any additional filters.

## Review Table Slot Actions

There's a bunch of different actions you can perform on a user. Right click their row to show a dropdown of different things to do:

- View Checks
  - Opens a checks table (seperated by sphere) for the slot, showing which locations they've found
- Password
  - View or change a slots password. Changing a slots password will kick any clients connected to that slot.
- Change Owner
  - Change the Discord ID which owns the slot in the lobby. This will also change the password and kick any clients connected to that slot.
- Set Alt Name
  - Adding names here will act as an alias to the slot name for the purposes of connecting. Not the same as !alias. Useful if a game can't input special characters but the slot name has them.
- Copy Patch URL
  - If a game has a patch file, this will be the url to download it from
- Toggle Bounce Isolation
  - If toggled on, clients connected to the slot will not be able to send bounces to, or receive bounces from different slots. Useful if there are crashes related to bad bounce packets.
- Toggle DeathLink Block
  - If toggled on, prevents DeathLink packets being sent to other slots. These will still count towards the Deaths counter.
- Goal Slot
  - The server will connect to the slot and sets the status to goaled
- Hint Item
  - Sends a hint via the admin console
- Give Item
  - Cheats in an item via the admin console
- Hint Location
  - Sends a hint for a location via the admin console
- Give Location
  - Gives a location (check) via the admin console
- Toggle Normal Access
  - If toggled on, allows access to the 'Normal Addr' as listed on the host page
- Open Debug Viewer
  - Shows a live feed of packets being sent from clients connected to the slot