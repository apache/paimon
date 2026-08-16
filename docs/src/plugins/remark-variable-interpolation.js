const plugin = () => {
  const config = require('../../docusaurus.config.js');
  const fields = config.customFields || {};

  const replacements = {
    '@@VERSION@@': fields.version || '',
    '@@FLINK_VERSION@@': fields.flinkVersion || '',
    '@@BRANCH@@': fields.branch || '',
    '@@GITHUB_REPO@@': fields.githubRepo || '',
    '@@TRINO_GITHUB_REPO@@': fields.trinoGithubRepo || '',
    '@@PRESTO_GITHUB_REPO@@': fields.prestoGithubRepo || '',
    '@@SCALA_VERSION@@': fields.scalaVersion || '',
  };

  const replaceTokens = (str) => {
    let result = str;
    for (const [token, value] of Object.entries(replacements)) {
      result = result.split(token).join(value);
    }
    return result;
  };

  const visitNode = (node) => {
    if (node.type === 'text' && node.value) {
      node.value = replaceTokens(node.value);
    }
    if (node.type === 'code' && node.value) {
      node.value = replaceTokens(node.value);
    }
    if (node.type === 'inlineCode' && node.value) {
      node.value = replaceTokens(node.value);
    }
    if (node.type === 'link' && node.url) {
      node.url = replaceTokens(node.url);
    }
    if (node.type === 'html' && node.value) {
      node.value = replaceTokens(node.value);
    }
    if (node.children) {
      node.children.forEach(visitNode);
    }
  };

  return (tree) => {
    visitNode(tree);
  };
};

module.exports = plugin;
