import React from 'react';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';

export default function AllVersions() {
  const {siteConfig} = useDocusaurusContext();
  const versions = siteConfig.customFields.previousDocs;
  return (
    <ul>
      {versions.map((v) => (
        <li key={v.label}>
          <a href={v.href}>{v.label}</a>
        </li>
      ))}
    </ul>
  );
}
