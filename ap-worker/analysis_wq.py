import asyncio
import collections
import logging
import multiprocessing
import os
import random
import shutil
import sys
import tempfile
import traceback
import uuid
from argparse import Namespace

sys.path.insert(0, "/ap/archipelago")

import aiohttp
import sentry_sdk
from opentelemetry import trace
from wq import LobbyQueue, JobStatus

import Utils
from Generate import main as GenMain, PlandoOptions
from settings import get_settings

ORIG_USER_PATH = Utils.user_path
tracer = trace.get_tracer("yaml-analyzer")

logger = logging.getLogger(__name__)

# Hardcoded apworlds required by fixed yamls
# Would be nice to not have to do this later
FIXED_YAML_APWORLDS = [
    ("hk", "0.6.7"),
]

async def main(loop):
    try:
        apworlds_dir = sys.argv[1]
        custom_apworlds_dir = sys.argv[2]
        fixed_yamls_dir = sys.argv[3]
    except IndexError:
        print("Usage: review_wq.py worlds_dir custom_worlds_dir fixed_yamls_dir")
        sys.exit(1)


    root_url = os.environ.get("LOBBY_ROOT_URL")
    if root_url is None:
        print("Please provide the lobby root url in `LOBBY_ROOT_URL`")
        sys.exit(1)

    aptools_root_url = os.environ.get("APTOOLS_ROOT_URL")
    if aptools_root_url is None:
        print("Please provide the lobby root url in `APTOOLS_ROOT_URL`")
        sys.exit(1)

    token = os.environ.get("YAML_ANALYSIS_QUEUE_TOKEN")
    if token is None:
        print("Please provide a token in `YAML_ANALYSIS_QUEUE_TOKEN`")
        sys.exit(1)

    api_key = os.environ.get("LOBBY_API_KEY")
    if api_key is None:
        print("Please provide an API key in `LOBBY_API_KEY`")
        sys.exit(1)

    fixed_yamls = [
        os.path.join(fixed_yamls_dir, f)
        for f in os.listdir(fixed_yamls_dir)
        if f.endswith(".yaml") or f.endswith(".yml")
    ]
    if not fixed_yamls:
        print(f"No yaml files found in fixed_yamls_dir {fixed_yamls_dir!r}")
        sys.exit(1)

    worker_name = str(uuid.uuid4())
    import handler
    ap_handler = handler.ApHandler(apworlds_dir, custom_apworlds_dir)
    await YamlAnalysisQueue(ap_handler, root_url, aptools_root_url, worker_name, token, loop, fixed_yamls).run()


class YamlAnalysisQueue(LobbyQueue):
    def __init__(self, ap_handler, root_url, aptools_root_url, worker_name, token, loop, fixed_yamls):
        super().__init__(aptools_root_url, "yaml_analysis", worker_name, token, loop)
        self.root_url = root_url
        self.aptools_root_url = aptools_root_url
        self.ap_handler = ap_handler
        self.fixed_yamls = fixed_yamls

    def handle_job(self, job):
        with tracer.start_as_current_span("yaml_analysis", context=job.ctx):
            status = JobStatus.Success
            result = None
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            try:
                room_id = job.params["room_id"]
                yaml_id = job.params["yaml_id"]

                all_apworlds = {(apworld, version) for apworld, version in job.params.get("apworlds", [])}
                all_apworlds.update(FIXED_YAML_APWORLDS)

                for apworld, version in all_apworlds:
                    print(apworld)
                    self.ap_handler.load_apworld(apworld, version)

                result = loop.run_until_complete(
                    self._analyze_yaml(room_id, yaml_id)
                )

            except Exception as e:
                traceback.print_exc()
                sentry_sdk.capture_exception(e)
                trace.get_current_span().record_exception(e)
                status = JobStatus.Failure
            finally:
                loop.close()
                asyncio.set_event_loop(None)

        return status, result

    async def _fetch_yaml(self, room_id: str, yaml_id: str) -> str:
        url = f"/room/{room_id}/download/{yaml_id}"
        async with aiohttp.ClientSession(self.root_url) as client:
            response = await client.get(url, headers={"X-Api-Key": os.environ["LOBBY_API_KEY"]})
            response.raise_for_status()
            return await response.text()

    async def _analyze_yaml(self, room_id: str, yaml_id: str) -> dict:
        players_dir = tempfile.mkdtemp(prefix="apanalysis_")
        try:
            # Get yaml file from lobby
            yaml_content = await self._fetch_yaml(room_id, yaml_id)

            # Player 1 = submitted yaml
            with open(os.path.join(players_dir, "Player1.yaml"), "w") as f:
                f.write(yaml_content)

            # Copy fixed yamls in as _Player2, _Player3, etc.
            for i, fixed_path in enumerate(self.fixed_yamls, start=2):
                dest = os.path.join(players_dir, f"Player{i}.yaml")
                shutil.copy(fixed_path, dest)

            total_players = 1 + len(self.fixed_yamls)

            settings = get_settings()
            args = Namespace(
                **{
                    "weights_file_path": settings.generator.weights_file_path,
                    "sameoptions": False,
                    "player_files_path": players_dir,
                    "seed": random.randint(10000, 10000000),
                    "multi": total_players,
                    "spoiler": 0,
                    "outputpath": None,
                    "outputname": None,
                    "spoiler_only": False,
                    "race": False,
                    "meta_file_path": None,
                    "log_level": "error",
                    "yaml_output": 0,
                    "plando": PlandoOptions.from_set(frozenset({"bosses", "items", "connections", "texts"})),
                    "skip_prog_balancing": True,
                    "skip_output": True,
                    "csv_output": False,
                    "log_time": False,
                }
            )

            loop = asyncio.get_running_loop()

            try:
                erargs, seed = await loop.run_in_executor(None, GenMain, args)
            except Exception as e:
                logger.warning("Generation setup failed: %s", e)
                sentry_sdk.capture_exception(e)
                return {
                    "player_name": None,
                    "game": None,
                    "total_checks": None,
                    "starting_checks": None,
                    "status": f"Generation setup failed: {e}",
                    "error": f"Generation setup failed: {e}",
                }

            try:
                from Main import main as ERmain
                multiworld = await loop.run_in_executor(None, ERmain, erargs, seed)
            except Exception as e:
                logger.warning("Generation failed: %s", e)
                sentry_sdk.capture_exception(e)
                return {
                    "player_name": None,
                    "game": None,
                    "total_checks": None,
                    "starting_checks": None,
                    "status": f"Generation failed: {e}",
                    "error": f"Generation failed: {e}",
                }

            # Only return metadata for player 1
            name = multiworld.player_name[1]
            game = multiworld.game[1]

            spheres = list(multiworld.get_spheres())     
                   
            sphere_1 = {
                loc for loc in spheres[0]
                if loc.player == 1 and loc.address is not None
            }
            locations = [
                loc for loc in multiworld.get_locations(1)
                if loc.address is not None
            ]
            if len(locations) == 0:
                return {
                    "player_name": name,
                    "game": game,
                    "total_checks": None,
                    "starting_checks": None,
                    "status": "No locations?",
                    "error": "No locations?",
                }

            return {
                "player_name": name,
                "game": game,
                "total_checks": len(locations),
                "starting_checks": len(sphere_1),
            }

        finally:
            shutil.rmtree(players_dir, ignore_errors=True)


if __name__ == "__main__":
    multiprocessing.set_start_method("fork")
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(main(loop))
    except KeyboardInterrupt:
        pass
