![Interlink logo](./docs/static/img/interlink_logo.png)

## :information_source: Overview

### Introduction
InterLink aims to provide an abstraction for the execution of a Kubernetes pod on any remote resource capable of managing a Container execution lifecycle.
We target to facilitate the development of provider specific plugins, so the resource providers can leverage the power of virtual kubelet without a black belt in kubernetes internals.

The project consists of two main components:

- __A Kubernetes Virtual Node:__ based on the [VirtualKubelet](https://virtual-kubelet.io/) technology. Translating request for a kubernetes pod execution into a remote call to the interLink API server.
- __The interLink API server:__ a modular and pluggable REST server where you can create your own Container manager plugin (called sidecars), or use the existing ones: remote docker execution on a remote host, singularity Container on a remote SLURM batch system. This repo aims to maintain the SLURM sidecar as a standalone plugin.

The project got inspired by the [KNoC](https://github.com/CARV-ICS-FORTH/knoc) and [Liqo](https://github.com/liqotech/liqo/tree/master) projects, enhancing that with the implemention a generic API layer b/w the virtual kubelet component and the provider logic for the container lifecycle management.

## :information_source: Usage

### Requirements
- __[Our Kubernetes Virtual Node and the interLink API server](https://github.com/interTwin-eu/interLink)__
- __[The Go programming language](https://go.dev/doc/install)__ (to build binaries)
- __[Docker Engine](https://docs.docker.com/engine/)__ (optional)

Note: if you want a quick start setup (using a Docker container), Go is not necessary

### Quick Start
