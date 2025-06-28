"use client";

import React, { useState } from 'react';
import { Activity, Users, Film, Calendar, ThumbsUp, Zap } from 'lucide-react';
import Link from 'next/link';

type SidebarProps = {
  activeTab: string;
  onTabChange: (tab: string) => void;
};

const Sidebar: React.FC<SidebarProps> = ({ activeTab, onTabChange }) => {
  const [isExpanded, setIsExpanded] = useState(true);

  const toggleSidebar = () => {
    setIsExpanded(!isExpanded);
  };

  const analyticsItems = [
    { id: 'overview', label: 'System', icon: <Activity className="w-4 h-4 mr-2" /> },
    { id: 'behavior', label: 'User', icon: <Users className="w-4 h-4 mr-2" /> },
    { id: 'content', label: 'Content', icon: <Film className="w-4 h-4 mr-2" /> },
    { id: 'temporal', label: 'Temporal', icon: <Calendar className="w-4 h-4 mr-2" /> },
  ];

  return (
    <div className={`h-screen bg-white shadow-md transition-all duration-300 ${isExpanded ? 'w-64' : 'w-24'}`}>
      <div className="flex justify-end p-2">
        <button 
          onClick={toggleSidebar}
          className="p-2 rounded-full hover:bg-slate-100"
        >
          {isExpanded ? '←' : '→'}
        </button>
      </div>

      <div className="p-4">
        <h2 className={`text-lg font-semibold mb-4 ${!isExpanded && 'hidden'}`}>Analytics</h2>
        <nav>
          <ul className="space-y-2">
            {analyticsItems.map((item) => (
              <li key={item.id}>
                <button
                  onClick={() => onTabChange(item.id)}
                  className={`flex items-center w-full p-2 rounded-md transition-colors ${
                    activeTab === item.id 
                      ? 'bg-slate-200 text-slate-800' 
                      : 'text-slate-600 hover:bg-slate-100'
                  }`}
                >
                  {item.icon}
                  {isExpanded && <span>{item.label}</span>}
                </button>
              </li>
            ))}
          </ul>
        </nav>
      </div>

      <div className="border-t border-slate-200 mt-4 p-4">
        <h2 className={`text-lg font-semibold mb-4 ${!isExpanded && 'hidden'}`}>Recommendations</h2>
        <Link href="/recommendations" className="flex items-center p-2 text-slate-600 hover:bg-slate-100 rounded-md">
          <ThumbsUp className="w-4 h-4 mr-2" />
          {isExpanded && <span>Try Recommendations</span>}
        </Link>
      </div>

      <div className="border-t border-slate-200 mt-4 p-4">
        <h2 className={`text-lg font-semibold mb-4 ${!isExpanded && 'hidden'}`}>Real-Time</h2>
        <Link href="/real-time" className="flex items-center p-2 text-slate-600 hover:bg-slate-100 rounded-md">
          <Zap className="w-4 h-4 mr-2" />
          {isExpanded && <span>Live Events</span>}
        </Link>
      </div>
    </div>
  );
};

export default Sidebar;
