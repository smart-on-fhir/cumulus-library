import json
import pathlib
import tempfile

import respx

from cumulus_library import note_utils


class MockModel:
    def __init__(self, model_id: str = "gpt-oss-120b", provider: str = "local"):
        self.model_id = model_id

        # Create a codebook for any NLP we do, to keep consistent anon IDs
        self.tempdir = tempfile.TemporaryDirectory()
        self.phi = pathlib.Path(self.tempdir.name, "phi")
        self.phi.mkdir()
        with open(self.phi / "codebook.json", "w", encoding="utf8") as f:
            salt = "e359191164cd209708d93551f481edd048946a9d844c51dea1b64d3f83dfd1fa"
            json.dump({"id_salt": salt}, f)

        # Determine the server to use for this model
        urls = {
            "gpt-oss-120b": "http://localhost:8086/v1",
        }
        self.url = urls[model_id]

        # Do some basic always-on mocking
        self.list_route = respx.get(f"{self.url}/models")
        self.chat_route = respx.post(f"{self.url}/chat/completions")
        self.mock_model_list()

    def cli_args(self) -> list[str]:
        return [
            f"--nlp-model={self.model_id}",
            f"--etl-phi-dir={self.phi}",
        ]

    def nlp_config(self) -> note_utils.NlpConfig:
        split_args = {
            k.split("=")[0].removeprefix("--").replace("-", "_"): k.split("=")[1]
            for k in self.cli_args()
        }
        return note_utils.NlpConfig(split_args)

    def mock_model_list(self) -> None:
        model_aliases = {
            "gpt-oss-120b": ["gpt-oss-120b", "openai.gpt-oss-120b-1:0", "openai/gpt-oss-120b"],
        }
        data = [{"id": alias} for alias in model_aliases.get(self.model_id, [])]

        self.list_route.respond(json={"object": "list", "data": data})

    def mock_response(self, value: dict) -> None:
        self.chat_route.respond(
            json={
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": json.dumps(value),
                        },
                        "finish_reason": "stop",
                    },
                ],
            },
        )
