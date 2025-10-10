How LLM Choose the Right MCP Tools
Guangya Liu
Guangya Liu

Follow
4 min read
·
May 9, 2025
14






In the fast-evolving landscape of artificial intelligence, Large Language Models (LLMs) like ChatGPT and Claude are playing an increasingly pivotal role in automating complex tasks. However, their true potential is unlocked when combined with tool invocation frameworks that allow them to interact with real-world systems. One such powerful framework is the Model Context Protocol (MCP).

This blog post provides a beginner-friendly yet technically rich explanation of how LLMs select the right MCP server and tool(s) to fulfill a given user request. Whether you’re an AI enthusiast, a developer, or someone new to automation, this guide will walk you through the fundamentals of MCP, the role of clients and servers, and how LLMs reason about tool usage.

If you’re new to MCP, I recommend starting with my previous blog MCP Developer Quick Start for a fast and practical introduction.

Step-by-Step Guide: How LLMs Select Tools via MCP
Step 1: Receiving the User Prompt
The process begins when a user interacts with an LLM and inputs a request, such as Diagnose high CPU usage on production server named as prod-server-23.

The LLM processes this natural language input and identifies that this is a task requiring an action beyond simple text generation.

Step 2: Accessing the Tool Catalog from MCP Server(s)
The LLM is either pre-configured or dynamically provided with a catalog of available tools. The best practice for now is let MCP Client get the tool catalog first and then send the tool catalog of MCP Server together with a query. This catalog includes:

Tool Name: E.g., diagnose_cpu_spike
Tool Description: E.g., Analyzes the CPU usage trend for a given server.
Input Parameters: E.g., host, duration
Return Type: What output the tool provides (e.g., JSON, string)
Constraints: Rate limits, required authentication, etc.
This metadata enables the LLM to reason about tool capabilities without hardcoding.

Step 3: Matching Tools to User Intent
Using its understanding of language and logic, the LLM compares the user query against available tool descriptions. This step is powered by few-shot examples, embeddings, and fine-tuned reasoning.

In our case, the LLM might shortlist tools like:

diagnose_cpu_spike
fetch_cpu_logs
It chooses the best match based on intent overlap and input/output fit.

Step 4: Constructing the Tool Invocation
Once a tool is selected, the LLM constructs a structured output. For example:

{
  "use_tool": "diagnose_cpu_spike",
  "params": {
    "host": "prod-server-23",
    "duration": "15m"
  }
}
This output is passed to the MCP client for execution.

Step 5: Client-Server Interaction
The MCP client is a middleware that routes requests from the LLM to the appropriate MCP server. It:

Authenticates the request
Validates the tool name and parameters
Sends the request to the right MCP server
The MCP server then executes the requested tool and returns the result (e.g., average CPU usage, a warning if thresholds are crossed).

Step 6: Returning the Result to the LLM
The MCP client forwards the server’s response back to the LLM. With this new context, the LLM can:

Generate a natural language summary
Display structured data in a friendly format
Chain further tool calls (e.g., “Send alert if CPU > 90%”)
Behind the Scenes: How the LLM Thinks
Language Understanding
The LLM uses embeddings and transformer layers to comprehend the user’s intent and parse relevant keywords like CPU, diagnose, or production.

Reasoning and Ranking
LLMs score available tools by measuring semantic similarity between the tool descriptions and the prompt. They may also consider tool constraints (e.g., do we have the required parameters?)

Context Awareness
LLMs remember prior steps. For example, if the previous step involved diagnosing disk usage, and now the user says Do the same for memory, the LLM can maintain continuity.

Example Walkthrough
Let’s see a full end-to-end example. User Input as Alert me if any server in the EU cluster is running low on memory.. The step-by-step execution will be as below:

Step 1: LLM identifies that this is a monitoring request.

Step 2: Tool catalog contains:

check_memory_usage
send_alert
Step 3: LLM first calls check_memory_usage:

{
  "use_tool": "check_memory_usage",
  "params": { "region": "eu-cluster" }
}
Step 4: MCP client sends this to the appropriate monitoring MCP server.

Step 5: Result returned:

{
  "low_memory_servers": ["eu-server-3", "eu-server-9"]
}
Step 6: LLM follows up with:

{
  "use_tool": "send_alert",
  "params": {
    "recipients": ["devops@company.com"],
    "message": "Low memory on eu-server-3 and eu-server-9"
  }
}
Step 7: Alert tool on messaging MCP server is triggered. Done.

Best Practices for MCP Tool Designers
Use Clear Descriptions: Make your tool name and description intuitive for the LLM to match user intent.
Avoid Ambiguity: If multiple tools sound similar, clarify their scopes (e.g., check_cpu_now vs. check_cpu_trend).
Parameter Naming Matters: Use meaningful parameter names like host, duration, severity.
Tool Chaining: Design tools to allow output-to-input chaining (e.g., diagnostic -> alert).
Auth and Constraints: Document usage limits and authentication needs clearly.
Future Outlook
As multi-agent AI ecosystems emerge, LLMs may soon:

Dynamically discover tools from federated MCP servers
Negotiate tool usage with authentication and quota awareness
Learn from tool usage history to recommend actions
Protocols like MCP are central to the next generation of Autonomous AI Agents that can execute tasks reliably across environments like DevOps, ITSM, finance, and security.

Summary
In a nutshell, LLMs use a combination of natural language understanding, tool description analysis, and structured function calling to choose the right MCP tool. The MCP protocol empowers this process by acting as a universal API layer between the LLM and the actionable backend systems.

If you’re building an AI Copilot, observability agent, or automation orchestrator, understanding how LLMs interact with MCP servers is the key to unleashing their full potential.
