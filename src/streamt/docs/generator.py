"""HTML documentation generator for streamt."""

from __future__ import annotations

import json
from pathlib import Path

from streamt.core.dag import DAG
from streamt.core.models import StreamtProject

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{project_name} - streamt docs</title>
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #0f0f0f;
            color: #e0e0e0;
            line-height: 1.6;
        }}

        .container {{
            max-width: 1400px;
            margin: 0 auto;
            padding: 2rem;
        }}

        header {{
            border-bottom: 1px solid #333;
            padding-bottom: 1rem;
            margin-bottom: 2rem;
        }}

        h1 {{
            font-size: 2rem;
            font-weight: 600;
            color: #fff;
        }}

        h2 {{
            font-size: 1.5rem;
            font-weight: 500;
            color: #fff;
            margin-top: 2rem;
            margin-bottom: 1rem;
        }}

        h3 {{
            font-size: 1.1rem;
            font-weight: 500;
            color: #aaa;
            margin-top: 1.5rem;
            margin-bottom: 0.5rem;
        }}

        .subtitle {{
            color: #888;
            font-size: 0.9rem;
        }}

        .stats {{
            display: flex;
            gap: 2rem;
            margin: 1.5rem 0;
            flex-wrap: wrap;
        }}

        .stat {{
            background: #1a1a1a;
            padding: 1rem 1.5rem;
            border-radius: 8px;
            border: 1px solid #333;
        }}

        .stat-value {{
            font-size: 2rem;
            font-weight: 600;
            color: #3b82f6;
        }}

        .stat-label {{
            color: #888;
            font-size: 0.85rem;
        }}

        .dag-container {{
            background: #1a1a1a;
            border: 1px solid #333;
            border-radius: 8px;
            padding: 2rem;
            margin: 1rem 0;
            overflow-x: auto;
        }}

        .dag-toggle {{
            display: flex;
            gap: 0.5rem;
            margin-bottom: 1rem;
        }}

        .dag-toggle button {{
            padding: 0.5rem 1rem;
            border: 1px solid #444;
            background: #1a1a1a;
            color: #888;
            border-radius: 4px;
            cursor: pointer;
            transition: all 0.2s;
        }}

        .dag-toggle button:hover {{
            border-color: #666;
            color: #fff;
        }}

        .dag-toggle button.active {{
            background: #3b82f6;
            border-color: #3b82f6;
            color: #fff;
        }}

        #dag-interactive {{
            width: 100%;
            height: 500px;
            border: 1px solid #333;
            border-radius: 4px;
            background: #111;
        }}

        #dag-ascii {{
            display: none;
        }}

        #dag-ascii pre {{
            background: #111;
            padding: 1rem;
            border-radius: 6px;
            overflow-x: auto;
            font-size: 0.85rem;
        }}

        .card-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(350px, 1fr));
            gap: 1rem;
            margin: 1rem 0;
        }}

        .card {{
            background: #1a1a1a;
            border: 1px solid #333;
            border-radius: 8px;
            padding: 1.25rem;
            transition: border-color 0.2s;
        }}

        .card:hover {{
            border-color: #3b82f6;
        }}

        .card-title {{
            font-weight: 500;
            color: #fff;
            margin-bottom: 0.5rem;
        }}

        .card-type {{
            display: inline-block;
            font-size: 0.75rem;
            padding: 0.25rem 0.5rem;
            border-radius: 4px;
            background: #333;
            color: #888;
            margin-bottom: 0.5rem;
        }}

        .card-type.source {{
            background: #1e3a5f;
            color: #60a5fa;
        }}

        .card-type.model {{
            background: #1e4620;
            color: #4ade80;
        }}

        .card-type.test {{
            background: #4a3520;
            color: #fbbf24;
        }}

        .card-type.exposure {{
            background: #3b1e4a;
            color: #c084fc;
        }}

        .card-description {{
            color: #888;
            font-size: 0.9rem;
            margin-top: 0.5rem;
        }}

        .card-meta {{
            display: flex;
            gap: 1rem;
            margin-top: 0.75rem;
            font-size: 0.8rem;
            color: #666;
        }}

        .tag {{
            display: inline-block;
            font-size: 0.7rem;
            padding: 0.15rem 0.4rem;
            border-radius: 3px;
            background: #333;
            color: #888;
            margin-right: 0.25rem;
        }}

        pre {{
            background: #111;
            padding: 1rem;
            border-radius: 6px;
            overflow-x: auto;
            font-size: 0.85rem;
            margin: 0.5rem 0;
        }}

        code {{
            font-family: 'SF Mono', Menlo, Monaco, monospace;
        }}

        .tabs {{
            display: flex;
            gap: 0;
            border-bottom: 1px solid #333;
            margin-bottom: 1rem;
            flex-wrap: wrap;
        }}

        .tab {{
            padding: 0.75rem 1.5rem;
            cursor: pointer;
            color: #888;
            border-bottom: 2px solid transparent;
            transition: all 0.2s;
        }}

        .tab:hover {{
            color: #fff;
        }}

        .tab.active {{
            color: #3b82f6;
            border-bottom-color: #3b82f6;
        }}

        .tab-content {{
            display: none;
        }}

        .tab-content.active {{
            display: block;
        }}

        /* D3 Graph Styles */
        .node {{
            cursor: pointer;
        }}

        .node circle {{
            stroke-width: 2px;
            transition: all 0.2s;
        }}

        .node:hover circle {{
            stroke-width: 3px;
            filter: brightness(1.2);
        }}

        .node text {{
            font-size: 11px;
            fill: #e0e0e0;
            pointer-events: none;
        }}

        .link {{
            fill: none;
            stroke: #444;
            stroke-width: 2px;
        }}

        .link-arrow {{
            fill: #444;
        }}

        .tooltip {{
            position: absolute;
            padding: 8px 12px;
            background: #1a1a1a;
            border: 1px solid #444;
            border-radius: 4px;
            font-size: 12px;
            color: #e0e0e0;
            pointer-events: none;
            z-index: 1000;
            max-width: 300px;
        }}

        .tooltip .name {{
            font-weight: 600;
            color: #fff;
            margin-bottom: 4px;
        }}

        .tooltip .type {{
            font-size: 10px;
            padding: 2px 6px;
            border-radius: 3px;
            display: inline-block;
            margin-bottom: 4px;
        }}

        .legend {{
            display: flex;
            gap: 1rem;
            margin-top: 1rem;
            flex-wrap: wrap;
        }}

        .legend-item {{
            display: flex;
            align-items: center;
            gap: 0.5rem;
            font-size: 0.8rem;
            color: #888;
        }}

        .legend-color {{
            width: 12px;
            height: 12px;
            border-radius: 50%;
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>{project_name}</h1>
            <p class="subtitle">streamt documentation</p>
        </header>

        <div class="stats">
            <div class="stat">
                <div class="stat-value">{source_count}</div>
                <div class="stat-label">Sources</div>
            </div>
            <div class="stat">
                <div class="stat-value">{model_count}</div>
                <div class="stat-label">Models</div>
            </div>
            <div class="stat">
                <div class="stat-value">{test_count}</div>
                <div class="stat-label">Tests</div>
            </div>
            <div class="stat">
                <div class="stat-value">{exposure_count}</div>
                <div class="stat-label">Exposures</div>
            </div>
        </div>

        <div class="tabs">
            <div class="tab active" onclick="showTab('dag')">DAG</div>
            <div class="tab" onclick="showTab('sources')">Sources</div>
            <div class="tab" onclick="showTab('models')">Models</div>
            <div class="tab" onclick="showTab('tests')">Tests</div>
            <div class="tab" onclick="showTab('exposures')">Exposures</div>
        </div>

        <div id="dag" class="tab-content active">
            <h2>Data Lineage</h2>
            <div class="dag-container">
                <div class="dag-toggle">
                    <button class="active" onclick="toggleDagView('interactive')">Interactive</button>
                    <button onclick="toggleDagView('ascii')">ASCII</button>
                </div>
                <div id="dag-interactive"></div>
                <div id="dag-ascii">
                    <pre>{dag_ascii}</pre>
                </div>
                <div class="legend">
                    <div class="legend-item">
                        <div class="legend-color" style="background: #60a5fa;"></div>
                        <span>Source</span>
                    </div>
                    <div class="legend-item">
                        <div class="legend-color" style="background: #4ade80;"></div>
                        <span>Model (topic)</span>
                    </div>
                    <div class="legend-item">
                        <div class="legend-color" style="background: #22d3ee;"></div>
                        <span>Model (flink)</span>
                    </div>
                    <div class="legend-item">
                        <div class="legend-color" style="background: #f472b6;"></div>
                        <span>Model (sink)</span>
                    </div>
                    <div class="legend-item">
                        <div class="legend-color" style="background: #c084fc;"></div>
                        <span>Exposure</span>
                    </div>
                </div>
            </div>
        </div>

        <div id="sources" class="tab-content">
            <h2>Sources</h2>
            <div class="card-grid">
                {sources_html}
            </div>
        </div>

        <div id="models" class="tab-content">
            <h2>Models</h2>
            <div class="card-grid">
                {models_html}
            </div>
        </div>

        <div id="tests" class="tab-content">
            <h2>Tests</h2>
            <div class="card-grid">
                {tests_html}
            </div>
        </div>

        <div id="exposures" class="tab-content">
            <h2>Exposures</h2>
            <div class="card-grid">
                {exposures_html}
            </div>
        </div>
    </div>

    <div class="tooltip" style="display: none;"></div>

    <script>
        // Tab switching
        function showTab(tabId) {{
            document.querySelectorAll('.tab').forEach(tab => tab.classList.remove('active'));
            document.querySelectorAll('.tab-content').forEach(content => content.classList.remove('active'));
            document.querySelector(`.tab[onclick="showTab('${{tabId}}')"]`).classList.add('active');
            document.getElementById(tabId).classList.add('active');
        }}

        // DAG view toggle
        function toggleDagView(view) {{
            document.querySelectorAll('.dag-toggle button').forEach(btn => btn.classList.remove('active'));
            event.target.classList.add('active');

            if (view === 'interactive') {{
                document.getElementById('dag-interactive').style.display = 'block';
                document.getElementById('dag-ascii').style.display = 'none';
            }} else {{
                document.getElementById('dag-interactive').style.display = 'none';
                document.getElementById('dag-ascii').style.display = 'block';
            }}
        }}

        // DAG Data
        const dagData = {dag_json};

        // Node colors by type
        const nodeColors = {{
            'source': '#60a5fa',
            'topic': '#4ade80',
            'virtual_topic': '#a3e635',
            'flink': '#22d3ee',
            'sink': '#f472b6',
            'exposure': '#c084fc'
        }};

        // Build graph data
        function buildGraphData(dag) {{
            const nodes = [];
            const links = [];
            const nodeMap = new Map();

            // Add sources
            if (dag.sources) {{
                dag.sources.forEach(source => {{
                    const node = {{
                        id: source,
                        name: source,
                        type: 'source',
                        color: nodeColors.source
                    }};
                    nodes.push(node);
                    nodeMap.set(source, node);
                }});
            }}

            // Add models
            if (dag.models) {{
                dag.models.forEach(model => {{
                    const matType = model.materialized || 'topic';
                    const node = {{
                        id: model.name,
                        name: model.name,
                        type: matType,
                        description: model.description || '',
                        color: nodeColors[matType] || nodeColors.topic
                    }};
                    nodes.push(node);
                    nodeMap.set(model.name, node);
                }});
            }}

            // Add exposures
            if (dag.exposures) {{
                dag.exposures.forEach(exposure => {{
                    const node = {{
                        id: exposure,
                        name: exposure,
                        type: 'exposure',
                        color: nodeColors.exposure
                    }};
                    nodes.push(node);
                    nodeMap.set(exposure, node);
                }});
            }}

            // Add edges
            if (dag.edges) {{
                dag.edges.forEach(edge => {{
                    if (nodeMap.has(edge.from) && nodeMap.has(edge.to)) {{
                        links.push({{
                            source: edge.from,
                            target: edge.to
                        }});
                    }}
                }});
            }}

            return {{ nodes, links }};
        }}

        // Initialize D3 graph
        function initGraph() {{
            const container = document.getElementById('dag-interactive');
            const width = container.clientWidth;
            const height = container.clientHeight || 500;

            // Clear existing
            container.innerHTML = '';

            const svg = d3.select('#dag-interactive')
                .append('svg')
                .attr('width', width)
                .attr('height', height);

            // Add zoom behavior
            const g = svg.append('g');

            const zoom = d3.zoom()
                .scaleExtent([0.3, 3])
                .on('zoom', (event) => g.attr('transform', event.transform));

            svg.call(zoom);

            // Arrow marker for directed edges
            svg.append('defs').append('marker')
                .attr('id', 'arrowhead')
                .attr('viewBox', '-0 -5 10 10')
                .attr('refX', 20)
                .attr('refY', 0)
                .attr('orient', 'auto')
                .attr('markerWidth', 6)
                .attr('markerHeight', 6)
                .append('path')
                .attr('d', 'M 0,-5 L 10 ,0 L 0,5')
                .attr('class', 'link-arrow');

            const {{ nodes, links }} = buildGraphData(dagData);

            if (nodes.length === 0) {{
                g.append('text')
                    .attr('x', width / 2)
                    .attr('y', height / 2)
                    .attr('text-anchor', 'middle')
                    .attr('fill', '#666')
                    .text('No nodes in DAG');
                return;
            }}

            // Force simulation
            const simulation = d3.forceSimulation(nodes)
                .force('link', d3.forceLink(links).id(d => d.id).distance(120))
                .force('charge', d3.forceManyBody().strength(-400))
                .force('center', d3.forceCenter(width / 2, height / 2))
                .force('collision', d3.forceCollide().radius(50));

            // Links
            const link = g.append('g')
                .selectAll('line')
                .data(links)
                .enter().append('line')
                .attr('class', 'link')
                .attr('marker-end', 'url(#arrowhead)');

            // Nodes
            const node = g.append('g')
                .selectAll('.node')
                .data(nodes)
                .enter().append('g')
                .attr('class', 'node')
                .call(d3.drag()
                    .on('start', dragstarted)
                    .on('drag', dragged)
                    .on('end', dragended));

            node.append('circle')
                .attr('r', 14)
                .attr('fill', d => d.color)
                .attr('stroke', d => d3.color(d.color).darker(0.5));

            node.append('text')
                .attr('dy', 25)
                .attr('text-anchor', 'middle')
                .text(d => d.name.length > 20 ? d.name.substring(0, 18) + '...' : d.name);

            // Tooltip
            const tooltip = d3.select('.tooltip');

            node.on('mouseover', (event, d) => {{
                tooltip
                    .style('display', 'block')
                    .style('left', (event.pageX + 10) + 'px')
                    .style('top', (event.pageY - 10) + 'px')
                    .html(`
                        <div class="name">${{d.name}}</div>
                        <div class="type" style="background: ${{d.color}}33; color: ${{d.color}};">${{d.type}}</div>
                        ${{d.description ? `<div style="margin-top: 4px;">${{d.description}}</div>` : ''}}
                    `);
            }})
            .on('mouseout', () => {{
                tooltip.style('display', 'none');
            }});

            // Simulation tick
            simulation.on('tick', () => {{
                link
                    .attr('x1', d => d.source.x)
                    .attr('y1', d => d.source.y)
                    .attr('x2', d => d.target.x)
                    .attr('y2', d => d.target.y);

                node.attr('transform', d => `translate(${{d.x}},${{d.y}})`);
            }});

            // Drag functions
            function dragstarted(event, d) {{
                if (!event.active) simulation.alphaTarget(0.3).restart();
                d.fx = d.x;
                d.fy = d.y;
            }}

            function dragged(event, d) {{
                d.fx = event.x;
                d.fy = event.y;
            }}

            function dragended(event, d) {{
                if (!event.active) simulation.alphaTarget(0);
                d.fx = null;
                d.fy = null;
            }}

            // Center initially
            svg.call(zoom.transform, d3.zoomIdentity.translate(0, 0).scale(0.9));
        }}

        // Initialize on load
        document.addEventListener('DOMContentLoaded', initGraph);
        window.addEventListener('resize', initGraph);
    </script>
</body>
</html>
"""


def generate_docs(project: StreamtProject, dag: DAG, output_path: Path) -> None:
    """Generate HTML documentation with interactive D3 DAG."""
    output_path.mkdir(parents=True, exist_ok=True)

    # Generate sources HTML
    sources_html = ""
    for source in project.sources:
        tags_html = "".join(f'<span class="tag">{tag}</span>' for tag in source.tags)
        sources_html += f"""
        <div class="card">
            <span class="card-type source">source</span>
            <h3 class="card-title">{source.name}</h3>
            <p class="card-description">{source.description or 'No description'}</p>
            <div class="card-meta">
                <span>Topic: {source.topic}</span>
                {f'<span>Owner: {source.owner}</span>' if source.owner else ''}
            </div>
            {f'<div style="margin-top: 0.5rem">{tags_html}</div>' if tags_html else ''}
        </div>
        """

    # Generate models HTML
    models_html = ""
    for model in project.models:
        tags_html = "".join(f'<span class="tag">{tag}</span>' for tag in model.tags)
        models_html += f"""
        <div class="card">
            <span class="card-type model">{model.get_materialized().value}</span>
            <h3 class="card-title">{model.name}</h3>
            <p class="card-description">{model.description or 'No description'}</p>
            <div class="card-meta">
                {f'<span>Owner: {model.owner}</span>' if model.owner else ''}
                <span>Access: {model.access.value}</span>
            </div>
            {f'<div style="margin-top: 0.5rem">{tags_html}</div>' if tags_html else ''}
        </div>
        """

    # Generate tests HTML
    tests_html = ""
    for test in project.tests:
        tests_html += f"""
        <div class="card">
            <span class="card-type test">{test.type.value}</span>
            <h3 class="card-title">{test.name}</h3>
            <div class="card-meta">
                <span>Model: {test.model}</span>
                <span>Assertions: {len(test.assertions)}</span>
            </div>
        </div>
        """

    # Generate exposures HTML
    exposures_html = ""
    for exposure in project.exposures:
        exposures_html += f"""
        <div class="card">
            <span class="card-type exposure">{exposure.type.value}</span>
            <h3 class="card-title">{exposure.name}</h3>
            <p class="card-description">{exposure.description or 'No description'}</p>
            <div class="card-meta">
                {f'<span>Owner: {exposure.owner}</span>' if exposure.owner else ''}
                {f'<span>Role: {exposure.role.value}</span>' if exposure.role else ''}
            </div>
        </div>
        """

    # Build DAG JSON for D3
    dag_dict = dag.to_dict()
    # Add model metadata for richer visualization
    dag_dict["models"] = [
        {
            "name": m.name,
            "materialized": m.get_materialized().value,
            "description": m.description or "",
        }
        for m in project.models
    ]
    dag_dict["exposures"] = [e.name for e in project.exposures]

    # Render HTML
    html = HTML_TEMPLATE.format(
        project_name=project.project.name,
        source_count=len(project.sources),
        model_count=len(project.models),
        test_count=len(project.tests),
        exposure_count=len(project.exposures),
        dag_ascii=dag.render_ascii(),
        dag_json=json.dumps(dag_dict),
        sources_html=sources_html or '<p style="color: #666">No sources defined</p>',
        models_html=models_html or '<p style="color: #666">No models defined</p>',
        tests_html=tests_html or '<p style="color: #666">No tests defined</p>',
        exposures_html=exposures_html or '<p style="color: #666">No exposures defined</p>',
    )

    # Write HTML file
    index_path = output_path / "index.html"
    with open(index_path, "w") as f:
        f.write(html)

    # Write manifest JSON
    manifest_path = output_path / "manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(
            {
                "project": project.project.name,
                "sources": [s.model_dump() for s in project.sources],
                "models": [m.model_dump() for m in project.models],
                "tests": [t.model_dump() for t in project.tests],
                "exposures": [e.model_dump() for e in project.exposures],
                "dag": dag.to_dict(),
            },
            f,
            indent=2,
            default=str,
        )
