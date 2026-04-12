---
layout: single
title: "Skills"
permalink: /skills/
author_profile: true
---

{% assign categories = site.skills | sort: "order" %}

{% for category in categories %}
<p class="section-label"{% if forloop.first == false %} style="margin-top:1.5rem;"{% endif %}>{{ category.title }}</p>
{% for item in category.items %}<span class="skill-chip">{{ item }}</span>
{% endfor %}
{% endfor %}
