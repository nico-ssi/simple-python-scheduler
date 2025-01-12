#!/usr/bin/env python

from os import name
import time
import random
import json

from kubernetes import client, config, watch

config.load_incluster_config()
v1 = client.CoreV1Api()
batch1 = client.BatchV1Api()

scheduler_name = "simple-python-scheduler"
NAMESPACE = "test-ns"


def nodes_available():
    ready_nodes = []
    for n in v1.list_node().items:
        for status in n.status.conditions:
            if status.status == "True" and status.type == "Ready":
                ready_nodes.append(n.metadata.name)
    return ready_nodes


def get_running_pods():
    pods = []
    for pod in v1.list_namespaced_pod(namespace=NAMESPACE).items:
        if pod.status.phase == "Running":
            pods.append(pod)
    return pods


def preemption(priority):
    for pod in get_running_pods():
        try:
            pod_priority = pod.metadata.annotations["priority"]
        except:
            print(f"Preempting pod {pod.metadata.name}")
            return v1.delete_namespaced_pod(name=pod.metadata.name, namespace=NAMESPACE)

        if int(pod_priority) > int(priority):
            print(f"Preempting pod {pod.metadata.name}")
            return v1.delete_namespaced_pod(name=pod.metadata.name, namespace=NAMESPACE)
    return None


def get_free_slots(priority):
    slots = 0
    for pod in get_running_pods():
        try:
            pod_priority = pod.metadata.annotations["priority"]
        except:
            slots += 1
        if int(pod_priority) < int(priority):
            slots += 1
    return slots


def scheduler(object, priority, node, namespace=NAMESPACE):
    try:
        job_name = object.metadata.labels["job-name"]
        job = batch1.read_namespaced_job(name=job_name, namespace=namespace)
        if job.spec.parallelism > get_free_slots(priority):
            return None
    except:
        pass

    while len(v1.list_node().items) <= len(get_running_pods()):
        preemption(priority)

    name = object.metadata.name
    target = client.V1ObjectReference(kind="Node", api_version="v1", name=node)
    meta = client.V1ObjectMeta(name=name)
    body = client.V1Binding(target=target, metadata=meta)
    return v1.create_namespaced_binding(
        namespace=namespace, body=body, _preload_content=False
    )


def main():
    w = watch.Watch()
    for event in w.stream(v1.list_namespaced_pod, NAMESPACE):
        if (
            event["object"].status.phase == "Pending"
            and event["object"].spec.scheduler_name == scheduler_name
        ):
            try:
                try:
                    priority = event["object"].metadata.annotations["priority"]
                except TypeError:
                    priority = 1000
                res = scheduler(
                    object=event["object"],
                    priority=priority,
                    node=random.choice(nodes_available()),
                )
            except client.rest.ApiException as e:
                print(json.loads(e.body)["message"])


if __name__ == "__main__":
    main()
