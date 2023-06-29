import os
import time
from unittest.mock import Mock

from pghoard.common import write_json_file
from pghoard.pghoard import PGHoard
from test.base import PGHoardTestCase


class TestConnectivityIssuesPGHoard(PGHoardTestCase):
    def setup_method(self, method):
        super().setup_method(method)
        self.test_site = "fe324d29-7745-4645-b43d-79b7d72c37f9"
        self.config = self.config_template(
            {
                "backup_sites": {
                    "fe324d29-7745-4645-b43d-79b7d72c37f9": {
                        "active": True,
                        "active_backup_mode": "nothing",
                        "basebackup_interval_hours": 1,
                        "basebackup_minute": 0,
                        "basebackup_mode": "local-tar-delta-stats",
                        "encryption_key_id": "kp3fo0v5uowcn57rynw6",
                        "encryption_keys": {
                            "kp3fo0v5uowcn57rynw6": {
                                "private": "-----BEGIN PRIVATE KEY-----\n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\nVnId5FzLj4P9UaShj0qT\n-----END PRIVATE KEY-----\n",
                                "public": "-----BEGIN PUBLIC KEY-----\nMIIBojANBgkqhkiG9w0BAQEFAAOCAY8AMIIBigKCAYEAugNL5bk2W51JV9avykav\nh/2PxpkNMr70qwIlpm3uPYABG7K9VmjZ1fz9R2C9fcTisnejXWkAkUTc2tsf6YmZ\nVpNHuGU1XW4OQ3zvaqMXC6yKqUKPrqEC5mlga2yKz7rz1oFDT1NrNH6JGO1gMrk/\nRMrWW9s4CUXT0DjHdXxPbIiSXMcnWn3GXSaND/IJrHtFCAErm1kaCyjUyZbrS0xd\nevLZETGj1jXqkW62UFio8qdeIZGWWjlnD9Mu9JYWUtKQ4QUoVN8EuqSVDsD/qA69\nIguP+kjwd3kshJyZk5zNERc8z1TEKcauXXsC4I8XRfXLpUgnmJGTvl1Re3WRLNrv\nLa6lWsbijCst+GAfGoAZkzv6y968FRmsk5j3tsBAHIppUX11UB/R1bdEeaoPVOx8\nLJpgY8sxog1jJEDib8Vl4zcqoz/tpqi2in/0z+or2DOTrnmMaYSyZew9wNdGRfnw\nm94McgLs4/OpuyrtYt/Lr2kkJxjd3V3RgJf1KUvL5o0fAgMBAAE=\n-----END PUBLIC KEY-----\n"
                            }
                        },
                        "nodes": [
                            {
                                "host": "127.0.0.4",
                            },
                        ],
                        "object_storage": {
                            "bucket_name": "kathia-barahon-test-p-d2b7e9d03689762895c787518a25079d320a43e82",
                            "credentials": {
                                "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                                "client_email": "project-bucket-account@aiven-test-remote.iam.gserviceaccount.com",
                                "client_id": "109659806506822090174",
                                "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/project-bucket-account%40aiven-test-remote.iam.gserviceaccount.com",
                                "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCW1v/Qx/0O8sxz\nV7y52PAbYoN0e0f9MJC7a6L9hGLK+yVx9Y3T2eH5qvk3dxjj3OncM8hqvtAbSnw5\n5YhyeHA03e6zlbH+8KtN4zv7Mb3fdXqtIGAyjLP+2Bo7+16Cl6k5+kUK7ATQFsnV\nuHqXiZoH8uLOAR1z91bMGTk05cvnPc6fkoyyN9VdPuN2HBxZIYeqn1WJqx8/ODWq\nAEtUnISZur5FYatILuWmNEnpj1dv1nF2AGXzwHw2EXZtrKWF2/aJAy71m+39g7CM\nNhFeQIgmojliw6pei5MBUXT/JyjfhLHQygGCrZZ/vQTt0LVGl8/5iFj6/zh3KH7m\nAXY9rhGFAgMBAAECggEAC67xhGYvjeKoJV/GKKfKyUQ5jGJajBwgsCbaxBCaAMDj\nmjUVdc0dkyB3Sm2GI9G7p3VQ8IiBFAWr8GWv/bXVP+Y+YL79v3jV7yPkd6tw2ofk\nWnHBVO9pdBoG+V7NNKF/13n9sqZFvsLVG7tWe4a9kIbZ1gnOEmppDBWaeCQ7q/IS\n8iYo2Vp0O4FSIlcE9Rxg+NHeGfo5/hA0W4W5um2ljbgNux+wjhiHcOhM5XqsW3g4\nAT0bAbVHo8Rg8sxqh0mZ4erHb+eCmERI2dQzPRcwUuu0N+kMSirLGTl+FF7PKt2X\nR8hsVEe8xVgN7ykX7PkzjZrtX8s+RM7H6s432nHDbQKBgQDOGaAmJHk+TGT6PTXY\nNOSLjwll41mSTfDYf4FSkjxfTLJlGc0S/ouPHw707pNVxE0nxjGIW8oJigFOOHBJ\n8/UQRM3fpYROWqLsAXtd6xJu71ubyRXA9oEtrBX2udFRhN+Wenq1rF3EWC6tobJC\nRgLgzGxhlK+dlWfxnlNgutoE5wKBgQC7XEPY8lcDxyIGpqaTkxb3JTdSFuzr1Gl2\n8K1dtxmxpKlxppoRWA6jmv+jqh/Kc1Yyce7oTfcgWHJm2E1ykk8XogHxUxxZWy5B\nNndFUC3lYxbZnOx8r4IBelHzPQJ4nB37yd+I2Ah/izJ4nyXZJIhw6m9SDI/zzMpU\nSw1V/+W8swKBgH4kAve1VRuDCD58Hitw2/xqlBbvGhBIccMf7tfJtveg6oKkUvZw\nIpx7Jt1T84sHtS1FyWUqwLIr6/ai5l7s2C2X3uUl2Z9XK+YEViw6RrLs/oWPgify\n90crztmOCwW4rFveJKJyl4Unb5JHp+GWFgbeNutWZFGvcsnX3DIUyoE7AoGABAUA\nt6DLWRtmkXn1zOi072xu+WXgg1a1RIX5Ui9hb7w2nmeSmpinB7+FiH9X20IKMV7c\nX0N878a1/ZraXoDhDYK+Q+0iiJA6N8/xUx1bPraXgOeq8ynYwitborpGWUwQIJy+\nHPN1izbzSD8x0qzD+Jgu9zWpyPM1zAUoLhYlWZ0CgYAcZNdRIB3YGsxfV4yxqSqt\nnaD4qoH97njt0ZkKJMMvUKreNTuQIwwZrgJdeYZkga0eJS6as7+wGIlT5FJ4VMNM\ntN50L8+IAtFdJ9jqSsNR1zv2KVz0uQ8fJaZLJI5+R7E5p8sQ6DJ9snZl1BYEwXWk\n/i2ySNZCrdy+c/5GCaxjag==\n-----END PRIVATE KEY-----\n",
                                "private_key_id": "0e9702f7cabbc543ec363789db42447cfff62374",
                                "project_id": "aiven-test-remote",
                                "token_uri": "https://oauth2.googleapis.com/token",
                                "type": "service_account"
                            },
                            "prefix": "kathia-barahon-test",
                            "project_id": "aiven-test-remote",
                            "storage_type": "google"
                        },
                        "pg_data_directory": os.path.join(str(self.temp_dir), "PG_DATA_DIRECTORY"),
                        "pg_receivexlog": {
                            "disk_space_check_interval": 10,
                            "min_disk_free_bytes": 10964959232,
                            "resume_multiplier": 1.5
                        },
                        "prefix": "402e5145-6c19-425e-aa24-0d35c3fa7e5b/fe324d29-7745-4645-b43d-79b7d72c37f9/15"
                    }
                },
                "compression": {
                    "algorithm": "zstd"
                },
            }
        )
        config_path = os.path.join(self.temp_dir, "pghoard.json")
        write_json_file(config_path, self.config)

        self.pghoard = PGHoard(config_path)
        self.real_check_pg_server_version = self.pghoard.check_pg_server_version
        self.pghoard.check_pg_server_version = Mock(return_value=90404)
        self.real_check_pg_versions_ok = self.pghoard.check_pg_versions_ok
        self.pghoard.check_pg_versions_ok = Mock(return_value=True)

    def teardown_method(self, method):
        self.pghoard.quit()
        self.pghoard.check_pg_server_version = self.real_check_pg_server_version
        self.pghoard.check_pg_versions_ok = self.real_check_pg_versions_ok
        super().teardown_method(method)

    def _get_next_wal_lsn(self, current_lsn: int) -> str:
        next_lsn_hex = hex(current_lsn + 1)
        return next_lsn_hex.split("x")[1].zfill(16).upper()

    def test_upload_with_bandwith_issues(self) -> None:
        compressed_wal_path, _ = self.pghoard.create_backup_site_paths(self.test_site)
        uncompressed_wal_path = compressed_wal_path + "_incoming"
        self.pghoard.run()
        current_lsn = -1
        max_uncompressed_wal_files = 200
        while True:
            current_uncompressed_wal_files = len(os.listdir(uncompressed_wal_path))
            if current_uncompressed_wal_files < max_uncompressed_wal_files:
                formatted_wal_lsn = self._get_next_wal_lsn(current_lsn)
                # fill file with crap (16 MB)
                with open(os.path.join(uncompressed_wal_path, formatted_wal_lsn), "wb") as out:
                    out.seek(16 * (1024 ** 2) - 1)
                    out.write('\0')

                current_lsn += 1
            #time.sleep(1)
