---
layout: archive
title: "Research"
permalink: /publications/
author_profile: true
---

<p class="section-label">Research & Publications</p>

My graduate research applied computational mathematics and statistical modeling to large-scale biological datasets.
The same methods — Bayesian inference, spatial statistics, clustering at scale — transfer directly to quantitative finance problems.

{% if author.googlescholar %}
  Google Scholar profile: <a href="{{author.googlescholar}}">{{ author.googlescholar }}</a>
{% endif %}

{% include base_path %}

{% for post in site.publications reversed %}
  {% include archive-single.html %}
{% endfor %}

---

<p class="section-label">Thesis</p>

**Identifying Immune Function in Vaping-Associated Pulmonary Injury via Spatial Transcriptomics**
M.S. Applied Mathematics, San Diego State University, 2023.
Advisors: Dr. Uduak George, Dr. Joy Philips, Ashley Schwartz.

A computational workflow combining dimensionality reduction, Bayesian clustering, and spatial gene-expression
analysis to map immune response at tissue resolution. Built in R (Seurat, STARsolo) and Python (scanpy, squidpy).
