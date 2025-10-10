# MCP Tool Quality Assessment Framework

## Overview
This framework evaluates MCP tools across multiple dimensions to ensure optimal LLM integration and user experience. Each tool receives an overall quality score (0-100) composed of weighted subscores.

## Quality Scoring Framework

### Overall Quality Score Calculation
```
Overall Score = (Naming × 0.25) + (Description × 0.25) + (Parameters × 0.20) + 
                (Schema Design × 0.15) + (LLM Friendliness × 0.15)
```

## 1. Naming Quality (Weight: 25%)

### Best Practices
- **Use clear, action-oriented verbs**: `performGoogleSearch` vs `googleSearch`
- **Preferred naming patter**: verb + object name   `searchTwitter` vs `twitterSearch`
- **Follow consistent naming conventions**: camelCase or snake_case consistently
- **Be specific about functionality**: `diagnose_cpu_spike` vs `check_cpu`
- **Avoid abbreviations**: `getUserProfile` vs `getUsrProf`


### Scoring Criteria (0-100)
- **90-100**: Perfect verb-noun structure, self-explanatory, follows conventions
- **70-89**: Clear intent, minor naming issues
- **50-69**: Understandable but could be clearer
- **30-49**: Ambiguous or inconsistent naming
- **0-29**: Confusing, misleading, or poorly named

### Examples
```json
// Excellent (Score: 95)
"name": "performGoogleSearch"

// Good (Score: 80)
"name": "search_web_content"

// Poor (Score: 30)
"name": "doStuff"
```

## 2. Description Quality (Weight: 25%)

### Best Practices
- **Comprehensive yet concise**: Explain what, why, and key capabilities
- **Include use cases**: When and how to use the tool
- **Mention key features**: Special capabilities, limitations, or options
- **Use clear language**: Avoid jargon, be beginner-friendly

### Scoring Criteria (0-100)
- **90-100**: Complete description with use cases, features, and context
- **70-89**: Clear description with most key information
- **50-69**: Basic description, missing some context
- **30-49**: Minimal or unclear description
- **0-29**: Missing, confusing, or misleading description

### Examples
```json
// Excellent (Score: 95)
"description": "Retrieves comprehensive Google search results including organic listings, featured snippets, knowledge graphs, and related content using the Serper API. Supports web search, image search, and news search with optional deep content extraction from result URLs."

// Good (Score: 75)
"description": "Searches Google and returns web results with titles, links, and snippets"

// Poor (Score: 25)
"description": "Search function"
```

## 3. Parameter Design Quality (Weight: 20%)

### Best Practices
- **Meaningful parameter names**: `requestBody__queries` vs `q`
- **Appropriate defaults**: Sensible default values where applicable
- **Clear parameter descriptions**: Explain purpose and expected values
- **Logical grouping**: Group related parameters consistently

### Scoring Criteria (0-100)
- **90-100**: All parameters well-named, documented, with appropriate defaults
- **70-89**: Most parameters clear, minor issues with naming or defaults
- **50-69**: Some parameters unclear or poorly named
- **30-49**: Many parameters confusing or undocumented
- **0-29**: Poor parameter design throughout

### Examples
```json
// Excellent (Score: 95)
"requestBody__deep_search": {
  "default": false,
  "description": "Enable deep search to crawl full content from search result URLs. When True, extracts and includes full page content in results for richer context. When False, only uses search snippets for faster response times. Note: Deep search significantly increases response time and API usage.",
  "type": "boolean"
}

// Poor (Score: 30)
"q": {
  "type": "string"
}
```

## 4. Schema Design Quality (Weight: 15%)

### Best Practices
- **Proper type definitions**: Use appropriate JSON schema types
- **Required vs optional**: Clear distinction with sensible required fields
- **Validation constraints**: Enums, patterns, min/max values where appropriate
- **Consistent structure**: Follow JSON Schema best practices

### Scoring Criteria (0-100)
- **90-100**: Perfect schema design with proper types, constraints, and structure
- **70-89**: Good schema with minor issues
- **50-69**: Basic schema, missing some validation
- **30-49**: Poor schema design with type issues
- **0-29**: Broken or severely flawed schema

### Examples
```json
// Excellent (Score: 95)
{
  "additionalProperties": false,
  "properties": {
    "requestBody__search_type": {
      "enum": ["search", "images", "news"],
      "type": "string"
    },
    "requestBody__page_size": {
      "default": 20,
      "type": "integer",
      "minimum": 1,
      "maximum": 100
    }
  },
  "required": ["requestBody__queries", "requestBody__limit"],
  "type": "object"
}
```

## 5. LLM Friendliness (Weight: 15%)

### Best Practices
- **Intent matching capability**: Tool name/description matches user intent patterns
- **Parameter inference**: LLM can easily map user input to parameters
- **Output format clarity**: Results are easy for LLM to interpret and present
- **Error handling**: Clear error messages for validation failures

### Scoring Criteria (0-100)
- **90-100**: Excellent LLM integration, natural intent matching
- **70-89**: Good LLM compatibility with minor issues
- **50-69**: Moderate LLM friendliness
- **30-49**: Difficult for LLM to use effectively
- **0-29**: Poor LLM integration, confusing for AI systems

## Quality Assessment Examples

### High Quality Tool (Overall Score: 92)
```json
{
  "_id": "68245dd381efb09d815ba9fe",
  "name": "performGoogleSearch",
  "description": "Retrieves comprehensive Google search results including organic listings, featured snippets, knowledge graphs, and related content using the Serper API. Supports web search, image search, and news search with optional deep content extraction from result URLs.",
  "server_name": "fastest_search",
  "quality_scores": {
    "overall": 92,
    "naming": 95,
    "description": 95,
    "parameters": 90,
    "schema": 88,
    "llm_friendliness": 90
  }
}
```

### Low Quality Tool (Overall Score: 35)
```json
{
  "name": "getData",
  "description": "Gets data",
  "server_name": "api_server",
  "input_schema": {
    "properties": {
      "q": {"type": "string"},
      "t": {"type": "string"}
    },
    "required": ["q"]
  },
  "quality_scores": {
    "overall": 35,
    "naming": 20,
    "description": 15,
    "parameters": 25,
    "schema": 40,
    "llm_friendliness": 30
  }
}
```

## Implementation Recommendations

### For Tool Developers
1. **Follow naming conventions**: Use clear, action-oriented names
2. **Write comprehensive descriptions**: Include use cases and key features
3. **Design intuitive parameters**: Use descriptive names and provide defaults
4. **Validate schemas thoroughly**: Test with various input combinations
5. **Consider LLM perspective**: Think about how AI will interpret your tool

### For Quality Review Process
1. **Automated scoring**: Implement automated checks for basic quality metrics
2. **Human review**: Manual assessment for description clarity and LLM friendliness
3. **User feedback integration**: Collect usage data to refine quality scores
4. **Continuous improvement**: Regular re-assessment of tool quality
5. **Quality gates**: Minimum score requirements for tool approval

### Quality Improvement Actions
- **Score 80+**: Approved for production use
- **Score 60-79**: Requires minor improvements before approval
- **Score 40-59**: Significant improvements needed
- **Score <40**: Major redesign required or tool rejection

## Monitoring and Maintenance

### Key Metrics to Track
- **Usage frequency**: How often tools are called by LLMs
- **Success rate**: Percentage of successful tool invocations
- **User satisfaction**: Feedback on tool effectiveness
- **LLM selection accuracy**: How often LLMs choose appropriate tools

### Quality Degradation Indicators
- Decreasing usage despite similar user intent patterns
- High error rates or failed invocations
- User complaints about tool effectiveness
- LLM confusion in tool selection scenarios
