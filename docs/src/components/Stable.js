import React from 'react';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';

export default function Stable({children}) {
  const {siteConfig} = useDocusaurusContext();
  if (!siteConfig.customFields.isStable) return null;
  return <>{children}</>;
}
