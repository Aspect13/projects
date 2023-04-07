#     Copyright 2020 getcarrier.io
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

from sqlalchemy import Column, Integer, String

from tools import db, db_tools, rpc_tools, MinioClient

from .project import Project


class Statistic(db_tools.AbstractBaseMixin, db.Base, rpc_tools.RpcMixin):
    __tablename__ = "statistic"

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, unique=False, nullable=False)
    start_time = Column(String(128), unique=False)
    vuh_used = Column(Integer, unique=False, default=0)
    performance_test_runs = Column(Integer, unique=False, default=0)
    sast_scans = Column(Integer, unique=False, default=0)
    dast_scans = Column(Integer, unique=False, default=0)
    public_pool_workers = Column(Integer, unique=False, default=0)
    ui_performance_test_runs = Column(Integer, unique=False, default=0)
    tasks_executions = Column(Integer, unique=False, default=0)

    def to_json(self, exclude_fields: tuple = ()) -> dict:
        json_dict = super().to_json()
        project = Project.query.get_or_404(json_dict["project_id"])
        minio_client = MinioClient(project=project)
        buckets_list = minio_client.list_bucket()
        storage_space = 0
        for bucket in buckets_list:
            for file in minio_client.list_files(bucket):
                storage_space += file["size"]
        json_dict["storage_space"] = round(storage_space/1000000, 2)
        json_dict["tasks_count"] = self.rpc.call.tasks_count(project_id=project.id)
        return json_dict
