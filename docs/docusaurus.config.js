// @ts-check

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'Apache Paimon',
  tagline: 'Data Lake Platform',
  favicon: 'favicon.png',

  url: 'https://paimon.apache.org',
  baseUrl: '/docs/master/',

  organizationName: 'apache',
  projectName: 'paimon',

  onBrokenLinks: 'warn',
  onBrokenAnchors: 'throw',

  markdown: {
    format: 'detect',
    mdx1Compat: {
      comments: true,
      admonitions: true,
      headingIds: true,
    },
  },

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  customFields: {
    version: '1.5-SNAPSHOT',
    versionTitle: '1.5-SNAPSHOT',
    branch: 'master',
    flinkVersion: '1.20',
    isStable: false,
    showOutDatedWarning: false,
    githubRepo: 'https://github.com/apache/paimon.git',
    trinoGithubRepo: 'https://github.com/apache/paimon-trino.git',
    prestoGithubRepo: 'https://github.com/apache/paimon-presto.git',
    scalaVersion: '_2.12',
    stableDocs: 'https://paimon.apache.org/docs/1.4',
    previousDocs: [
      {label: 'master', href: 'https://paimon.apache.org/docs/master'},
      {label: 'stable (1.4)', href: 'https://paimon.apache.org/docs/1.4'},
      {label: '1.3', href: 'https://paimon.apache.org/docs/1.3'},
      {label: '1.2', href: 'https://paimon.apache.org/docs/1.2'},
      {label: '1.1', href: 'https://paimon.apache.org/docs/1.1'},
    ],
  },

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          routeBasePath: '/',
          sidebarPath: './sidebars.js',
          editUrl: 'https://github.com/apache/paimon/edit/master/docs/',
          beforeDefaultRemarkPlugins: [
            require('./src/plugins/remark-variable-interpolation'),
          ],
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      }),
    ],
  ],

  themes: [
    [
      require.resolve('@easyops-cn/docusaurus-search-local'),
      {
        hashed: true,
        language: ['en'],
        indexBlog: false,
        docsRouteBasePath: '/',
      },
    ],
  ],

  plugins: [
    [
      '@docusaurus/plugin-client-redirects',
      {
        redirects: require('./redirects'),
      },
    ],
    function rawLoaderPlugin() {
      return {
        name: 'raw-loader-plugin',
        configureWebpack() {
          return {
            module: {
              rules: [
                {
                  test: /generated\/.*\.html$/,
                  type: 'asset/source',
                },
              ],
            },
          };
        },
      };
    },
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      navbar: {
        title: 'Apache Paimon',
        logo: {
          alt: 'Apache Paimon Logo',
          src: 'paimon_black.svg',
          srcDark: 'paimon_white.svg',
        },
        items: [
          {
            type: 'dropdown',
            label: '1.5-SNAPSHOT',
            position: 'right',
            items: [
              {label: 'master (1.5-SNAPSHOT)', href: 'https://paimon.apache.org/docs/master'},
              {label: '1.4 (stable)', href: 'https://paimon.apache.org/docs/1.4'},
              {label: '1.3', href: 'https://paimon.apache.org/docs/1.3'},
              {label: '1.2', href: 'https://paimon.apache.org/docs/1.2'},
              {label: '1.1', href: 'https://paimon.apache.org/docs/1.1'},
            ],
          },
          {
            href: 'https://github.com/apache/paimon',
            label: 'GitHub',
            position: 'right',
          },
          {
            href: 'https://paimon.apache.org',
            label: 'Project Home',
            position: 'right',
          },
        ],
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'Documentation',
            items: [
              {label: 'Getting Started', to: '/flink/quick-start'},
              {label: 'Concepts', to: '/concepts/overview'},
            ],
          },
          {
            title: 'Community',
            items: [
              {label: 'GitHub', href: 'https://github.com/apache/paimon'},
              {label: 'Mailing List', href: 'https://paimon.apache.org/community/mailing-lists'},
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} The Apache Software Foundation. Apache Paimon, Paimon, and the Paimon logo are trademarks of the Apache Software Foundation.`,
      },
      prism: {
        theme: require('prism-react-renderer').themes.github,
        darkTheme: require('prism-react-renderer').themes.dracula,
        additionalLanguages: ['java', 'scala', 'sql', 'bash', 'json', 'yaml', 'markup', 'properties'],
      },
      docs: {
        sidebar: {
          hideable: true,
          autoCollapseCategories: true,
        },
      },
    }),
};

module.exports = config;
