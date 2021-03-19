#
# Copyright 2018-2021 IBM Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import os

from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.utils.decorators import apply_defaults
from airflow_notebook import __version__
from typing import Dict, List, Optional
"""
The NotebookOp uses a python script to bootstrap the user supplied image with the required dependencies.
In order for the script run properly, the image used, must at a minimum, have the 'curl' utility available
and have python3
"""

# Inputs and Outputs separator character.  If updated,
# same-named variable in bootstrapper.py must be updated!
INOUT_SEPARATOR = ';'

ELYRA_GITHUB_ORG = os.getenv("ELYRA_GITHUB_ORG", "elyra-ai")
ELYRA_GITHUB_BRANCH = os.getenv("ELYRA_GITHUB_BRANCH", "master" if 'dev' in __version__ else "v" + __version__)

ELYRA_BOOTSTRAP_SCRIPT_URL = os.getenv('ELYRA_BOOTSTRAP_SCRIPT_URL', 'https://raw.githubusercontent.com/{org}/'
                                                                     'airflow-notebook/{branch}/etc/docker-scripts/'
                                                                     'bootstrapper.py'.
                                                                     format(org=ELYRA_GITHUB_ORG,
                                                                            branch=ELYRA_GITHUB_BRANCH))

ELYRA_REQUIREMENTS_URL = os.getenv('ELYRA_REQUIREMENTS_URL', 'https://raw.githubusercontent.com/{org}/'
                                                             'airflow-notebook/{branch}/etc/requirements-elyra.txt'.
                                                             format(org=ELYRA_GITHUB_ORG,
                                                                    branch=ELYRA_GITHUB_BRANCH))


@apply_defaults
class NotebookOp(KubernetesPodOperator):
    def __init__(self,
                 notebook: str,
                 cos_endpoint: str,
                 cos_bucket: str,
                 cos_directory: str,
                 cos_dependencies_archive: str,
                 pipeline_outputs: Optional[List[str]] = None,
                 pipeline_inputs: Optional[List[str]] = None,
                 requirements_url: str = None,
                 bootstrap_script_url: str = None,
                 *args, **kwargs) -> None:

        """Create a new instance of ContainerOp.
        Args:
          notebook: name of the notebook that will be executed per this operation
          cos_endpoint: object storage endpoint e.g weaikish1.fyre.ibm.com:30442
          cos_bucket: bucket to retrieve archive from
          cos_directory: name of the directory in the object storage bucket to pull
          cos_dependencies_archive: archive file name to get from object storage bucket e.g archive1.tar.gz
          pipeline_outputs: comma delimited list of files produced by the notebook
          pipeline_inputs: comma delimited list of files to be consumed/are required by the notebook
          pipeline_envs: dictionary of environmental variables to set in the container prior to execution
          requirements_url: URL to a python requirements.txt file to be installed prior to running the notebook
          bootstrap_script_url: URL to a custom python bootstrap script to run
          kwargs: additional key value pairs to pass e.g. name, image, sidecars & is_exit_handler.
                  See Kubeflow pipelines ContainerOp definition for more parameters or how to use
                  https://kubeflow-pipelines.readthedocs.io/en/latest/source/kfp.dsl.html#kfp.dsl.ContainerOp
        """

        self.notebook = notebook
        self.notebook_name = self.notebook_name = os.path.basename(notebook)
        self.cos_endpoint = cos_endpoint
        self.cos_bucket = cos_bucket
        self.cos_directory = cos_directory
        self.cos_dependencies_archive = cos_dependencies_archive
        self.container_work_dir_root_path = "./"
        self.container_work_dir_name = "jupyter-work-dir/"
        self.container_work_dir = self.container_work_dir_root_path + self.container_work_dir_name
        self.bootstrap_script_url = bootstrap_script_url
        self.requirements_url = requirements_url
        self.pipeline_outputs = pipeline_outputs
        self.pipeline_inputs = pipeline_inputs

        argument_list = []

        if not self.bootstrap_script_url:
            self.bootstrap_script_url = ELYRA_BOOTSTRAP_SCRIPT_URL

        if not self.requirements_url:
            self.requirements_url = ELYRA_REQUIREMENTS_URL

        if 'image' not in kwargs:
            raise ValueError("You need to provide an image.")

        if not notebook:
            raise ValueError("You need to provide a notebook.")

        if 'arguments' not in kwargs:
            """ If no arguments are passed, we use our own.
                If ['arguments'] are set, we assume container's ENTRYPOINT is set and dependencies are installed
                NOTE: Images being pulled must have python3 available on PATH and cURL utility
            """

            argument_list.append("mkdir -p {container_work_dir} && cd {container_work_dir} && "
                                 "curl -H 'Cache-Control: no-cache' -L {bootscript_url} --output bootstrapper.py && "
                                 "curl -H 'Cache-Control: no-cache' -L {reqs_url} --output requirements-elyra.txt && "
                                 "python3 -m pip install packaging && "
                                 "python3 -m pip freeze > requirements-current.txt && "
                                 "python3 bootstrapper.py "
                                 "--cos-endpoint {cos_endpoint} "
                                 "--cos-bucket {cos_bucket} "
                                 "--cos-directory '{cos_directory}' "
                                 "--cos-dependencies-archive '{cos_dependencies_archive}' "
                                 "--file '{notebook}' "
                                 .format(container_work_dir=self.container_work_dir,
                                         bootscript_url=self.bootstrap_script_url,
                                         reqs_url=self.requirements_url,
                                         cos_endpoint=self.cos_endpoint,
                                         cos_bucket=self.cos_bucket,
                                         cos_directory=self.cos_directory,
                                         cos_dependencies_archive=self.cos_dependencies_archive,
                                         notebook=self.notebook
                                         )
                                 )

            if self.pipeline_inputs:
                inputs_str = self._artifact_list_to_str(self.pipeline_inputs)
                argument_list.append("--inputs '{}' ".format(inputs_str))

            if self.pipeline_outputs:
                outputs_str = self._artifact_list_to_str(self.pipeline_outputs)
                argument_list.append("--outputs '{}' ".format(outputs_str))

            kwargs['cmds'] = ['sh', '-c']
            kwargs['arguments'] = [''.join(argument_list)]

        super().__init__(*args, **kwargs)

    def _artifact_list_to_str(self, pipeline_array):
        trimmed_artifact_list = []
        for artifact_name in pipeline_array:
            if INOUT_SEPARATOR in artifact_name:  # if INOUT_SEPARATOR is in name, throw since this is our separator
                raise \
                    ValueError("Illegal character ({}) found in filename '{}'.".format(INOUT_SEPARATOR, artifact_name))
            trimmed_artifact_list.append(artifact_name.strip())
        return INOUT_SEPARATOR.join(trimmed_artifact_list)
