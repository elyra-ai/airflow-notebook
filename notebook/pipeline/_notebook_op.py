#
# Copyright 2018-2020 IBM Corporation
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
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.utils.decorators import apply_defaults


class NotebookOp(KubernetesPodOperator):

    @apply_defaults
    def __init__(self,
                 notebook: str,
                 cos_endpoint: str,
                 cos_bucket: str,
                 cos_directory: str,
                 cos_pull_archive: str,
                 pipeline_inputs: str,
                 pipeline_outputs: str,
                 bootstrap_script_url: str = None,
                 *args, **kwargs) -> None:

        """Create a new instance of ContainerOp.
        Args:
          cos_endpoint: object store endpoint e.g weaikish1.fyre.ibm.com:30442
          cos_bucket: bucket to retrieve archive from
          cos_pull_archive: archive file name to get from object store bucket e.g archive1.tar.gz
        """

        self.notebook = notebook
        self.notebook_name = \
            self._get_file_name_with_extension(notebook, 'ipynb')
        self.notebook_result = \
            self._get_file_name_with_extension(notebook + '_output', 'ipynb')
        self.notebook_html = \
            self._get_file_name_with_extension(notebook + '_output', 'html')
        self.cos_endpoint = cos_endpoint
        self.cos_bucket = cos_bucket
        self.cos_directory = cos_directory
        self.cos_pull_archive = cos_pull_archive
        self.container_work_dir = "jupyter-work-dir"
        self.pipeline_inputs = pipeline_inputs
        self.cos_endpoint = cos_endpoint
        self.cos_dir_pre = cos_directory + '/'
        self.pipeline_outputs = pipeline_outputs
        self.bootstrap_script_url = bootstrap_script_url

        if self.bootstrap_script_url is None:
            """ If bootstrap_script arg with URL not provided, use the one baked in here.
            """
            self.bootstrap_script_url = 'https://raw.githubusercontent.com/elyra-ai/' \
                                        'airflow-notebook/master/etc/docker-scripts/bootstrapper.py'

        if 'image' not in kwargs:  # default image used if none specified
            kwargs['image'] = 'elyra/tensorflow:1.15.2-py3'

        if self.notebook is None:
            ValueError("You need to provide a notebook.")

        if 'arguments' not in kwargs:
            """ If no arguments are passed, we use our own.
                If ['arguments'] are set, we assume container's ENTRYPOINT is set and dependencies are installed
                NOTE: Images being pulled must have python3 available on PATH and cURL utility
            """

            kwargs['cmds'] = ['sh', '-c']
            kwargs['arguments'] = ['mkdir -p ./%s && cd ./%s && '
                                   'curl -H "Cache-Control: no-cache" -L %s --output bootstrapper.py && '
                                   'python bootstrapper.py '
                                   ' --endpoint %s '
                                   ' --bucket %s '
                                   ' --directory %s '
                                   ' --tar-archive %s '
                                   ' --pipeline-outputs %s '
                                   ' --pipeline-inputs %s '
                                   ' --input %s '
                                   ' --output %s '
                                   ' --output-html %s' % (
                                       self.container_work_dir,
                                       self.container_work_dir,
                                       self.bootstrap_script_url,
                                       self.cos_endpoint,
                                       self.cos_bucket,
                                       self.cos_directory,
                                       self.cos_pull_archive,
                                       self.pipeline_outputs,
                                       self.pipeline_inputs,
                                       self.notebook_name,
                                       self.notebook_result,
                                       self.notebook_html
                                       )  # noqa E123
                                   ]

        super().__init__(*args, **kwargs)

    def _get_file_name_with_extension(self, name, extension):
        """Simple function to construct a string filename
        Args:
            name: name of the file
            extension: extension to append to the name
        Returns:
            name_with_extension: string filename
        """
        name_with_extension = name
        if extension not in name_with_extension:
            name_with_extension = '{}.{}'.format(name, extension)

        return name_with_extension
