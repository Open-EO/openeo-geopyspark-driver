import copy
import textwrap
from importlib.resources import files

import pytest
import yaml
from jinja2 import ChoiceLoader, DictLoader, Environment, FileSystemLoader

from openeogeotrellis.integrations.kubernetes import (
    k8s_finalize_pod_templates,
    k8s_job_name,
    k8s_render_manifest_template,
    k8s_state_to_openeo_job_status,
)


def test_k8s_job_name():
    assert k8s_job_name().startswith("a-")
    assert len(k8s_job_name()) <= 35  # has a limit, previous implementation was 35 chars long
    assert k8s_job_name() != k8s_job_name()


def test_k8s_state_to_openeo_job_status():
    assert "queued" == k8s_state_to_openeo_job_status("foobar")
    assert "queued" == k8s_state_to_openeo_job_status("PENDING")
    assert "queued" == k8s_state_to_openeo_job_status("")
    assert "queued" == k8s_state_to_openeo_job_status("SUBMITTED")
    assert "running" == k8s_state_to_openeo_job_status("RUNNING")
    assert "finished" == k8s_state_to_openeo_job_status("COMPLETED")
    assert "error" == k8s_state_to_openeo_job_status("FAILED")
    assert "error" == k8s_state_to_openeo_job_status("FAILING")
    assert "running" == k8s_state_to_openeo_job_status("SUCCEEDING")


PODTEMPLATE_BASE = "sparkapplication_podtemplate_base.yaml.j2"
LEGACY_BASE = "sparkapplication_template.yaml.j2"

_RENDER_KWARGS = dict(
    job_name="a-123",
    job_namespace="spark-jobs",
    job_id_full="j-123",
    job_id_short="123",
    user_id="john",
    user_id_short="john",
    batch_scheduler="default-scheduler",
    python_max_memory="1000",
    propagatable_web_app_driver_envars={"SOME_SECRET": "s3cr3t"},
    py_files=["https://example.com/lib.zip", "https://example.com/helper.py"],
    layer_catalog_init_image="DISABLED",
    eodata_mount="/eodata",
    open_telemetry_prometheus_port=None,
)

_RENDER_OPTIONS = {
    "defaults": {},
    "yunikorn": {"batch_scheduler": "yunikorn", "yunikorn_queue": "root.q", "yunikorn_scheduling_timeout": "60"},
    "mount_tmp": {"mount_tmp": True},
    "use_pvc": {"use_pvc": True},
    "fuse_mount_per_job_pvc": {"fuse_mount_batchjob_s3_bucket": True},
    "fuse_mount_shared_pvc": {"fuse_mount_batchjob_s3_bucket": True, "shared_results_pvc": "results"},
    "s3_profiles": {
        "provide_s3_profiles_and_tokens": True,
        "batch_job_cfg_secret_name": "cfg-a-123",
        "batch_job_config_dir": "/opt/job_config",
    },
    "layercatalog_init": {
        "layer_catalog_init_image": "lc:1",
        "layer_catalog_init_dir": "/opt/layercatalogs",
        "layer_catalog_init_pull_policy": "IfNotPresent",
    },
    "layercatalog_initdata": {
        "layer_catalog_init_image": "lc:1",
        "layer_catalog_init_dir": "/initdata/layercatalogs",
        "layer_catalog_init_pull_policy": "IfNotPresent",
        "initdata_dir": "/initdata",
    },
    "everything": {
        "batch_scheduler": "yunikorn",
        "mount_tmp": True,
        "use_pvc": True,
        "fuse_mount_batchjob_s3_bucket": True,
        "shared_results_pvc": "results",
        "provide_s3_profiles_and_tokens": True,
        "batch_job_cfg_secret_name": "cfg-a-123",
        "batch_job_config_dir": "/opt/job_config",
        "layer_catalog_init_image": "lc:1",
        "layer_catalog_init_dir": "/initdata/layercatalogs",
        "initdata_dir": "/initdata",
        "separate_asset_per_band_new_partitioner": "True",
        "force_s3proxy": True,
    },
}

# Fields of spec.{driver,executor} that the operator webhook applies, and where they go in a pod template.
_CONTAINER_FIELDS = {
    "env": "env",
    "envFrom": "envFrom",
    "volumeMounts": "volumeMounts",
    "ports": "ports",
    "lifecycle": "lifecycle",
    "securityContext": "securityContext",
}
_POD_FIELDS = {
    "initContainers": "initContainers",
    "podSecurityContext": "securityContext",
    "terminationGracePeriodSeconds": "terminationGracePeriodSeconds",
}


def _legacy_to_pod_templates(app: dict) -> dict:
    """Test helper: move what the operator webhook applies from a legacy rendering into pod templates."""
    app = copy.deepcopy(app)
    spec = app["spec"]
    volumes = spec.pop("volumes", None) or []
    for role, container_name in [("driver", "spark-kubernetes-driver"), ("executor", "spark-kubernetes-executor")]:
        role_spec = spec[role]
        container = {"name": container_name}
        pod_spec = {"containers": [container], "volumes": copy.deepcopy(volumes)}
        for src, dst in _CONTAINER_FIELDS.items():
            if role_spec.get(src) is not None:
                container[dst] = role_spec.pop(src)
            role_spec.pop(src, None)
        for src, dst in _POD_FIELDS.items():
            if role_spec.get(src) is not None:
                pod_spec[dst] = role_spec.pop(src)
            role_spec.pop(src, None)
        role_spec["template"] = {"spec": pod_spec}
    return app


@pytest.mark.parametrize("options", _RENDER_OPTIONS.values(), ids=_RENDER_OPTIONS.keys())
def test_podtemplate_base_parity_with_legacy_base(options):
    """The pod template base results in the same pods as the legacy base configured through the operator webhook."""
    kwargs = {**_RENDER_KWARGS, **options}
    legacy = _legacy_to_pod_templates(k8s_render_manifest_template(LEGACY_BASE, **kwargs))
    k8s_finalize_pod_templates(legacy)
    new = k8s_render_manifest_template(PODTEMPLATE_BASE, **kwargs)
    k8s_finalize_pod_templates(new)
    assert new == legacy


@pytest.mark.parametrize("options", _RENDER_OPTIONS.values(), ids=_RENDER_OPTIONS.keys())
def test_podtemplate_base_findings(options):
    app = k8s_render_manifest_template(PODTEMPLATE_BASE, **{**_RENDER_KWARGS, **options})
    findings = k8s_finalize_pod_templates(app)
    # The base keeps the legacy monitoring config, the deployment has to provide the prometheus config file
    assert findings == [
        "monitoring.prometheus without both configFile and metricsPropertiesFile:"
        " the operator webhook still has to mount its prometheus configmap"
    ]
    for role in ["driver", "executor"]:
        role_spec = app["spec"][role]
        assert not {"env", "envFrom", "volumeMounts", "initContainers", "podSecurityContext", "lifecycle"} & set(
            role_spec
        )
        assert [c["name"] for c in role_spec["template"]["spec"]["containers"]] == [f"spark-kubernetes-{role}"]
    assert "volumes" not in app["spec"]


def test_podtemplate_base_volumes_per_pod():
    app = k8s_render_manifest_template(
        PODTEMPLATE_BASE,
        **{**_RENDER_KWARGS, "provide_s3_profiles_and_tokens": True, "batch_job_cfg_secret_name": "cfg"},
    )
    k8s_finalize_pod_templates(app)
    assert [v["name"] for v in app["spec"]["driver"]["template"]["spec"]["volumes"]] == [
        "eodata",
        "shared-pod-volume",
        "batch-job-cfg",
    ]


def _render_child(child: str, **kwargs) -> dict:
    """Render a custom template (like a deployment's sparkapplication.yaml.j2) that extends the base."""
    deploy_dir = str(files("openeogeotrellis.deploy"))
    env = Environment(loader=ChoiceLoader([DictLoader({"child.j2": child}), FileSystemLoader(deploy_dir)]))
    return yaml.safe_load(env.get_template("child.j2").render(**{**_RENDER_KWARGS, **kwargs}))


def test_podtemplate_base_child_overrides_at_column_0():
    child = textwrap.dedent("""\
        {% extends "sparkapplication_podtemplate_base.yaml.j2" %}
        {% block driver_volumeMounts %}
        {{ super() }}
        - name: prometheus-config
          mountPath: /etc/metrics/conf/metrics.properties
          subPath: metrics.properties
        - name: not-defined
          mountPath: /nope
        {% endblock %}
        {% block executor_volumeMounts %}
        {{ super() }}
        - name: prometheus-config
          mountPath: /etc/metrics/conf/metrics.properties
          subPath: metrics.properties
        {% endblock %}
        {% block volumes %}
        {{ super() }}
        - name: prometheus-config
          configMap:
            name: prometheus-config
        - name: unused
          emptyDir: {}
        {% endblock %}
        {% block driver_env %}
        {{ super() }}
        - name: NODE_IP
          valueFrom:
            fieldRef:
              fieldPath: status.hostIP
        {% endblock %}
        {% block driver_envFrom %}
        envFrom:
          - secretRef:
              name: s3-credentials
        {% endblock %}
        {% block driver_labels %}
        {{ super() }}
        role: driver
        {% endblock %}
        {% block sparkConf %}
        {{ super() }}
        "spark.eventLog.dir": "s3a://history"
        {% endblock %}
        {% block monitoring %}
        {{ super() }}
        metricsPropertiesFile: /etc/metrics/conf/metrics.properties
        prometheus:
          configFile: /opt/prometheus_config.yaml
          jmxExporterJar: "/opt/jmx_prometheus_javaagent-0.13.0.jar"
          port: 8090
        {% endblock %}
        """)
    app = _render_child(child)
    findings = k8s_finalize_pod_templates(app)
    assert findings == ["spec.driver: dropped mount /nope of undefined volume 'not-defined'"]

    spec = app["spec"]
    assert spec["sparkConf"]["spark.eventLog.dir"] == "s3a://history"
    assert spec["monitoring"]["prometheus"]["configFile"] == "/opt/prometheus_config.yaml"
    assert spec["driver"]["labels"]["role"] == "driver"
    driver_pod = spec["driver"]["template"]["spec"]
    driver = driver_pod["containers"][0]
    assert driver["envFrom"] == [{"secretRef": {"name": "s3-credentials"}}]
    assert driver["env"][-1] == {"name": "NODE_IP", "valueFrom": {"fieldRef": {"fieldPath": "status.hostIP"}}}
    assert {"name": "jmx-exporter", "containerPort": 8090, "protocol": "TCP"} in driver["ports"]
    assert [v["name"] for v in driver_pod["volumes"]] == ["eodata", "shared-pod-volume", "prometheus-config"]
    executor = spec["executor"]["template"]["spec"]["containers"][0]
    assert [p["name"] for p in executor["ports"]] == ["prom-scala", "prom-python", "jmx-exporter"]


def test_finalize_pod_templates_without_templates_is_noop():
    app = k8s_render_manifest_template(LEGACY_BASE, **_RENDER_KWARGS)
    expected = copy.deepcopy(app)
    assert k8s_finalize_pod_templates(app) == []
    assert app == expected


def test_finalize_pod_templates_reports_webhook_only_fields():
    app = {
        "spec": {
            "volumes": [{"name": "data", "emptyDir": {}}, {"name": "spark-local-dir-1", "emptyDir": {}}],
            "driver": {
                "env": [{"name": "A", "value": "1"}],
                "template": {
                    "spec": {
                        "containers": [
                            {
                                "name": "spark-kubernetes-driver",
                                "env": [{"name": "B", "value": "1"}, {"name": "B", "value": "2"}],
                                "volumeMounts": [
                                    {"name": "data", "mountPath": "/data"},
                                    {"name": "data", "mountPath": "/data"},
                                    {"name": "spark-local-dir-1", "mountPath": "/tmp"},
                                ],
                            }
                        ],
                        "initContainers": [{"name": "init", "volumeMounts": [{"name": "missing", "mountPath": "/m"}]}],
                    }
                },
            },
            "executor": {"template": {"spec": {"containers": [{"name": "executor"}]}}},
        }
    }
    findings = k8s_finalize_pod_templates(app)
    assert findings == [
        "spec.volumes is only applied by the operator webhook, render volumes in the pod templates instead",
        "spec.driver fields only applied by the operator webhook: env",
        "spec.driver: dropped mount /tmp of Spark local dir volume 'spark-local-dir-1'",
        "spec.driver: init container 'init' mounts undefined volume 'missing'",
        "spec.driver: duplicate volume mount paths ['/data']",
        "spec.driver: duplicate env vars ['B']",
        "spec.executor.template should have exactly one container 'spark-kubernetes-executor', got ['executor']",
    ]
    assert app["spec"]["driver"]["template"]["spec"]["volumes"] == [{"name": "data", "emptyDir": {}}]
    assert "volumes" not in app["spec"]["executor"]["template"]["spec"]


def test_finalize_pod_templates_metrics_properties_not_mounted():
    app = {
        "spec": {
            "monitoring": {
                "metricsPropertiesFile": "/etc/metrics/conf/metrics.properties",
                "prometheus": {"configFile": "/opt/prometheus.yaml", "port": 9999},
            },
            "driver": {"template": {"spec": {"containers": [{"name": "spark-kubernetes-driver"}]}}},
            "executor": {
                "template": {
                    "spec": {
                        "containers": [
                            {
                                "name": "spark-kubernetes-executor",
                                "ports": [{"name": "jmx-exporter", "containerPort": 8090}],
                                "volumeMounts": [{"name": "metrics", "mountPath": "/etc/metrics/conf"}],
                            }
                        ],
                        "volumes": [{"name": "metrics", "configMap": {"name": "metrics"}}],
                    }
                }
            },
        }
    }
    assert k8s_finalize_pod_templates(app) == [
        "spec.driver: metricsPropertiesFile /etc/metrics/conf/metrics.properties is not mounted",
        "spec.executor: jmx-exporter port differs from monitoring.prometheus.port 9999",
    ]
    assert app["spec"]["driver"]["template"]["spec"]["containers"][0]["ports"] == [
        {"name": "jmx-exporter", "containerPort": 9999, "protocol": "TCP"}
    ]


def test_finalize_pod_templates_tolerates_unnamed_entries():
    app = {
        "spec": {
            "driver": {
                "template": {
                    "spec": {
                        "containers": [{"name": "spark-kubernetes-driver", "volumeMounts": [{"mountPath": "/x"}]}],
                        "initContainers": [{"name": "init", "volumeMounts": [{"mountPath": "/y"}]}],
                        "volumes": [{"emptyDir": {}}],
                    }
                }
            },
            "executor": {"template": {"spec": {"containers": [{"name": "spark-kubernetes-executor"}]}}},
        }
    }
    assert k8s_finalize_pod_templates(app) == [
        "spec.driver: dropped mount /x of undefined volume ''",
        "spec.driver: init container 'init' mounts undefined volume ''",
    ]
