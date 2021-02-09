
import uuid
import time

from kubernetes import client, config


class Kube:

    def __init__(self):

        config.load_kube_config()
        self.deployed = {}
        self.api_instance = client.ExtensionsV1beta1Api()

    def get_modules(self):
        mods = []
        ret = client.list_pod_for_all_namespaces(watch=False)
        for i in ret.items:
            print("%s\t%s\t%s" %
                  (i.status.pod_ip, i.metadata.namespace, i.metadata.name))
            print(i)
        return mods

    def deploy_module(self, modules, image, workflow, replicas=1):
        name = str(uuid.uuid1())
        module = image[:image.find(":")]
        args = []
        if modules:
            args.extend(["--modules", ",".join(modules)])

        container = client.V1Container(
            name=module,
            image=image,
            args=args)

        template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(labels={
                "app": "cryocloud",
                "workflow": workflow,
                "module": module
            }),
            spec=client.V1PodSpec(containers=[container])
        )

        spec = client.ExtensionsV1beta1DeploymentSpec(
            replicas=replicas,
            template=template)

        deployment = client.ExtensionsV1beta1Deployment(
            api_version="extensions/v1beta1",
            kind="Deployment",
            metadata=client.V1ObjectMeta(name=name),
            spec=spec)

        api_response = self.api_instance.create_namespaced_deployment(
            body=deployment,
            namespace="default")

        print("Deployment %s created for modules %s" % (name, modules))

        for m in modules:
            if m not in self.deployed:
                self.deployed[m] = []
            self.deployed[m].append(name)

    def stop_deployment(self, deployment):
        api_response = self.api_instance.delete_namespaced_deployment(
            name=deployment,
            namespace="default",
            body=client.V1DeleteOptions(
                propagation_policy='Foreground',
                grace_period_seconds=5))
        print("Deployment stopped")

    def stop_module_deployment(self, module):
        if module in self.deployed:
            print("Found deployments for module", module)
            for name in self.deployed[module]:
                print("STOPPING", name)
                self.stop_deployment(name)
        else:
            print("No deployments for module", module)
            print(self.deployed)

    def stop_all_deployments(self):
        for module in self.deployed:
            for name in self.deployed[module]:
                self.stop_deployment(name)


if __name__ == '__main__':

    k = Kube()
    try:
        # k.deploy_module(["SAR_Processor", "SLC_Preprocessor"], "localhost:32000/cryoniteocean:latest", "TestWorkflow")
        k.deploy_module(["AIOilDetect"], "localhost:32000/cryoniteocean:latest", "TestWorkflow")
        while True:
            time.sleep(10)
    finally:
        k.stop_all_deployments()
