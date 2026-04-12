---
layout: single
title: "Notes"
permalink: /notes/
author_profile: true
---

<p class="section-label">Reading Notes &amp; Write-ups</p>

<p style="font-size:0.9em; color:var(--df-text-muted); margin-bottom:2rem;">
  Notes on quant finance, mathematics, and software — papers I've read, concepts I've worked through, and things worth writing down.
</p>

{% assign posts_by_year = site.posts | group_by_exp: "post", "post.date | date: '%Y'" %}

{% if site.posts.size == 0 %}
  <p style="color:var(--df-text-muted); font-style:italic;">No notes yet — check back soon.</p>
{% else %}
  {% for year_group in posts_by_year %}
  <p class="section-label" style="margin-top:2rem;">{{ year_group.name }}</p>
  <ul class="notes-list">
    {% for post in year_group.items %}
    <li class="note-entry">
      <div class="note-meta">{{ post.date | date: "%b %d" }}</div>
      <div class="note-body">
        <a href="{{ post.url | relative_url }}" class="note-title">{{ post.title }}</a>
        {% if post.tags.size > 0 %}
        <div class="note-tags">
          {% for tag in post.tags %}<span class="project-tag">{{ tag }}</span> {% endfor %}
        </div>
        {% endif %}
        {% if post.excerpt %}<p class="note-excerpt">{{ post.excerpt | strip_html | truncate: 160 }}</p>{% endif %}
      </div>
    </li>
    {% endfor %}
  </ul>
  {% endfor %}
{% endif %}
