# Mobility Analysis Workflow

MAWpy is a Python package designed for mobility analysis workflows, providing
tools for handling cellular/GPS traces and related data.

This repository contains scripts for processing user cellular/gps trace data
using various techniques to improve data quality and accuracy. The pipeline
involves several steps to handle trace segmentation, incremental clustering,
address oscillation and update stay duration.

## Overview

The pipeline consists of the following processing steps:

1. **Incremental Clustering**: Clusters traces based on a spatial threshold to
   identify potential stay points.
2. **Update Stay Duration**: Updates the duration of identified stays.
3. **Address Oscillation**: Handles oscillations in traces to ensure accurate
   stay detection.
4. **Trace Segmentation Clustering**: Segments traces and clusters them based on
   spatial and duration constraints.

## Prerequisites

- Python >=3.11

## Installation As a Package

To install MAWpy, you can use one of the following methods:

### Using PyPI

The simplest way to install MAWpy is via PyPI using `pip`. This will install the
package along with its dependencies:

```bash
pip install mawpy
```

## Read The Docs:

Please follow the link below to find the further documentation for MAWpy:
[mawpy.readthedocs.io/](mawpy.readthedocs.io/)

## Developer Guide

To contribute to MAWpy as a developer, please follow the setup instructions at
[CONTRIBUTING.md](.github/CONTRIBUTING.md)
