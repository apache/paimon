import React from 'react';

export default function ConfigTable({html}) {
  return (
    <div
      className="config-table-wrapper"
      dangerouslySetInnerHTML={{__html: html}}
    />
  );
}
